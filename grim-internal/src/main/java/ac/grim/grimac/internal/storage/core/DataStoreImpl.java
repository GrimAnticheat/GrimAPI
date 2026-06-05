package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Shared-impl {@link DataStore}. Hosts a per-category Disruptor ring via
 * {@link RingRegistry}, routes submits to the right ring via {@link CategoryRouter},
 * and runs reads on a small shared executor so the caller thread (e.g. netty or
 * main) doesn't block on backend latency.
 */
@ApiStatus.Internal
public final class DataStoreImpl implements DataStore {

    private final CategoryRouter router;
    private final RingRegistry rings;
    private final ExecutorService reader;
    private final AggregateDataStoreMetrics metrics;
    private final long shutdownDrainTimeoutMs;
    private final Logger logger;
    private volatile boolean closed;

    /**
     * v2 routing table — categories registered on the new
     * {@code BackendV2}/{@code KindAdapter} SPI. Initially empty; populated
     * via {@link #withV2Routes(V2Routes)} during startup wiring.
     */
    private volatile V2Routes v2Routes = V2Routes.empty();

    public DataStoreImpl(CategoryRouter router, WritePathConfig writePath, Logger logger) {
        this.router = router;
        this.logger = logger;
        this.rings = new RingRegistry(writePath, logger);
        this.metrics = new AggregateDataStoreMetrics(rings);
        this.shutdownDrainTimeoutMs = writePath.shutdownDrainTimeoutMs();
        AtomicInteger readerSeq = new AtomicInteger();
        this.reader = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "grim-datastore-reader-" + readerSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    /** Install v2 routes (categories backed by {@code BackendV2}/{@code KindAdapter}). */
    public DataStoreImpl withV2Routes(@NotNull V2Routes v2Routes) {
        this.v2Routes = v2Routes;
        return this;
    }

    public V2Routes v2Routes() { return v2Routes; }

    /**
     * Wires one ring per routed category. Must be called after
     * {@link CategoryRouter} is populated, backends are {@code init}ed,
     * and {@link #withV2Routes(V2Routes)} has been called (if v2
     * bootstrap is enabled). For categories with a v2 route, the ring's
     * writeHandler comes from the v2 adapter so writes land in the v7
     * physical layout. Otherwise the legacy backend's writeHandler is
     * used.
     */
    public void start() {
        try {
            // Register rings for legacy-routed categories
            for (Category<?> cat : router.routedCategories()) registerRingFor(cat);
            // Register rings for v2-only categories (not in the legacy
            // router but present in v2Routes). This covers the v2-only
            // cutover mode where the CategoryRouter is empty.
            if (!v2Routes.isEmpty()) {
                for (Category<?> cat : v2RoutedCategories()) {
                    if (!rings.hasRing(cat)) registerRingFor(cat);
                }
            }
        } catch (BackendException e) {
            throw new RuntimeException("failed to start DataStore ring", e);
        }
    }

    /** Categories registered in v2Routes. Used by start() to ensure rings exist. */
    private java.util.Set<Category<?>> v2RoutedCategories() {
        // V2Routes doesn't expose its key set. We probe the known
        // builtin categories. Extension categories (Phase 3+) would
        // need a V2Routes.categories() accessor.
        java.util.Set<Category<?>> out = new java.util.LinkedHashSet<>();
        for (Category<?> c : new Category<?>[] {
                Categories.VIOLATION, Categories.SESSION,
                Categories.PLAYER_IDENTITY, Categories.SETTING }) {
            if (v2Routes.contains(c)) out.add(c);
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <E> void registerRingFor(Category<E> cat) throws BackendException {
        V2Routes.Route<?> route = v2Routes.routeFor(cat);
        if (route != null) {
            rings.register(cat, route.backend(), (KindAdapter) route.adapter(),
                route.storeId(), route.kind());
        } else {
            Backend b = router.backendFor(cat);
            rings.register(cat, b);
        }
    }

    @Override
    public <E> void submit(Category<E> cat, Consumer<E> configurer) {
        if (closed) return;
        if (cat == Categories.VIOLATION) {
            submitViolation(configurer);
            return;
        }
        rings.submit(cat, configurer);
    }

    @SuppressWarnings("unchecked")
    private <E> void submitViolation(Consumer<E> configurer) {
        rings.submit(Categories.VIOLATION, event -> {
            ((Consumer<ViolationEvent>) configurer).accept(event);
            if (event.id() == null) event.id(UuidV7.next());
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <R> CompletionStage<R> execute(@NotNull Operation<R> op) {
        if (closed) {
            CompletableFuture<R> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("DataStore is closed"));
            return f;
        }
        Category<?> cat = op.category();
        V2Routes.Route<? extends DataKind<?, ?>> route = v2Routes.routeFor(cat);
        if (route == null) {
            CompletableFuture<R> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException(
                "no v2 route registered for category " + cat.id()
                    + " — operation " + op.getClass().getSimpleName() + " cannot dispatch"));
            return f;
        }
        boolean isReadShaped = isReadOperation(op);
        return CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            try {
                KindAdapter adapter = route.adapter();
                return (R) adapter.execute(route.storeId(), route.kind(), op);
            } catch (BackendException e) {
                throw new RuntimeException("v2 execute failed for " + op.getClass().getSimpleName(), e);
            } finally {
                long ms = (System.nanoTime() - start) / 1_000_000;
                // Only read-shaped ops feed the read-latency metric. Write-shaped
                // Operation<Void> deletes would otherwise pollute that histogram.
                if (isReadShaped) metrics.observeReadLatencyMs(ms);
            }
        }, reader);
    }

    /**
     * Heuristic — Operations whose name starts with a known write prefix are
     * write-shaped. A more principled split (e.g. an interface marker on
     * Operation subtypes) is a future cleanup; this lets the metric stay
     * honest in the meantime.
     * <p>
     * Covered write-shaped op names today (across all per-Kind op families):
     * Delete*, Remove*, Put*, IncrementBy*, SetIfHigher*.
     */
    private static boolean isReadOperation(@NotNull Operation<?> op) {
        String n = op.getClass().getSimpleName();
        return !(n.startsWith("Delete")
              || n.startsWith("Remove")
              || n.startsWith("Put")
              || n.startsWith("IncrementBy")
              || n.startsWith("SetIfHigher"));
    }

    @Override
    public <R> CompletionStage<Page<R>> query(Category<?> cat, Query<R> query) {
        // v2 transparent dispatch: if a v2 route exists for this
        // category AND we know how to translate the legacy Query type
        // into a v2 Operation, dispatch through execute(). This
        // transparently migrates every consumer of query() without
        // touching each consumer file.
        V2Routes.Route<?> route = v2Routes.routeFor(cat);
        if (route != null) {
            Operation<?> v2Op = QueryToOperationTranslator.translate(cat, query);
            if (v2Op != null) {
                return translateV2Result(execute(v2Op), query);
            }
        }
        // Legacy fallback: either no v2 route, or the Query type has
        // no v2 translation yet (e.g. GetSetting, or a custom Query).
        Backend b = router.backendFor(cat);
        if (b == null) {
            CompletableFuture<Page<R>> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException(
                "no backend (v2 or legacy) for category " + cat.id()
                    + " — query " + query.getClass().getSimpleName() + " cannot dispatch"));
            return f;
        }
        return CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            try {
                return b.read(cat, query);
            } catch (BackendException e) {
                throw new RuntimeException("backend read failed", e);
            } finally {
                metrics.observeReadLatencyMs((System.nanoTime() - start) / 1_000_000);
            }
        }, reader);
    }

    /**
     * Adapt the v2 execute() result into a Page<R> that the query()
     * contract returns. v2 Operations that return Optional<R> are wrapped
     * into a singleton or empty page; those that already return Page<R>
     * pass through.
     */
    @SuppressWarnings("unchecked")
    private <R> CompletionStage<Page<R>> translateV2Result(
            CompletionStage<?> future, Query<R> originalQuery) {
        return future.thenApply(raw -> {
            if (raw instanceof Page<?> page) return (Page<R>) page;
            if (raw instanceof Optional<?> opt) {
                // GetSetting returns Optional<byte[]> from the v2 KV
                // GetOp, but the legacy contract is Page<SettingRecord>.
                // Wrap the raw bytes back into a SettingRecord using
                // the query's scope/scopeKey/key so existing callers
                // (PlayerToggleStoreImpl etc.) see the same shape.
                if (originalQuery instanceof ac.grim.grimac.api.storage.query.Queries.GetSetting g) {
                    if (opt.isEmpty()) return Page.empty();
                    Object rawValue = opt.get();
                    byte[] bytes = rawValue instanceof byte[] bs ? bs
                        : rawValue instanceof org.bson.types.Binary b ? b.getData()
                        : rawValue instanceof org.bson.BsonBinary bb ? bb.getData()
                        : null;
                    if (bytes == null) {
                        throw new IllegalStateException(
                            "unexpected SETTING value type: " + rawValue.getClass().getName());
                    }
                    ac.grim.grimac.api.storage.model.SettingRecord rec =
                        new ac.grim.grimac.api.storage.model.SettingRecord(
                            g.scope(), g.scopeKey(), g.key(), bytes, System.currentTimeMillis());
                    return new Page<>(java.util.List.of((R) rec), null);
                }
                return opt.isPresent()
                    ? new Page<>(java.util.List.of((R) opt.get()), null)
                    : Page.empty();
            }
            // Shouldn't reach here if the translator is correct.
            throw new IllegalStateException(
                "unexpected v2 execute result type: " + raw.getClass().getName()
                    + " for query " + originalQuery.getClass().getSimpleName());
        });
    }

    @Override
    public <E> CompletionStage<Void> delete(Category<E> cat, DeleteCriteria criteria) {
        V2Routes.Route<?> route = v2Routes.routeFor(cat);
        if (route != null) {
            Operation<Void> v2Op = DeleteCriteriaTranslator.translate(cat, criteria);
            if (v2Op != null) {
                // If the routed adapter doesn't yet support this Op
                // (e.g. RedisEntityAdapter.DeleteByIndexOp before secondary
                // indexes land), fall back to the legacy Backend below.
                // We probe synchronously inside the v2 future to avoid
                // double-dispatching on the normal success path.
                return execute(v2Op).handle((v, ex) -> {
                    if (ex == null) return CompletableFuture.<Void>completedFuture(null);
                    // Look anywhere in the cause chain for a UOE: the
                    // adapter rethrows UOE wrapped in BackendException,
                    // execute() further wraps that in RuntimeException,
                    // and CompletableFuture adds CompletionException on
                    // top — three layers between us and the original
                    // UOE.
                    if (findUoe(ex) != null) {
                        return legacyDelete(cat, criteria);
                    }
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    f.completeExceptionally(ex);
                    return f;
                }).thenCompose(s -> s);
            }
        }
        return legacyDelete(cat, criteria);
    }

    private <E> CompletionStage<Void> legacyDelete(Category<E> cat, DeleteCriteria criteria) {
        Backend b = router.backendFor(cat);
        if (b == null) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException(
                "no backend (v2 or legacy) for category " + cat.id()
                    + " — delete cannot dispatch (criteria: " + criteria.getClass().getSimpleName() + ")"));
            return f;
        }
        return CompletableFuture.runAsync(() -> {
            try {
                b.delete(cat, criteria);
            } catch (BackendException e) {
                throw new RuntimeException("backend delete failed", e);
            }
        }, reader);
    }

    /**
     * Walk the full cause chain looking for an
     * {@link UnsupportedOperationException}. Adapters typically rethrow
     * the UOE wrapped in a {@code BackendException} and we wrap that in a
     * {@code RuntimeException} inside {@link #execute}, so the UOE sits
     * 2-3 layers below the top exception the future surfaces. Returns
     * {@code null} if the chain has no UOE; the {@code delete()} caller
     * falls through to the unconditional-rethrow path in that case.
     */
    private static @Nullable UnsupportedOperationException findUoe(@NotNull Throwable t) {
        Throwable cur = t;
        int depth = 0;
        while (cur != null && depth++ < 32) {
            if (cur instanceof UnsupportedOperationException uoe) return uoe;
            Throwable next = cur.getCause();
            if (next == null || next == cur) return null;
            cur = next;
        }
        return null;
    }

    @Override
    public CompletionStage<DeletionReport> forgetPlayer(UUID uuid) {
        // Run synchronously on the reader thread to avoid deadlocking
        // the 2-thread reader pool: .join() on sub-tasks submitted to
        // the same pool can starve. Instead, call the synchronous
        // adapter/backend methods directly.
        return CompletableFuture.supplyAsync(() -> {
            try {
                int sessions = 0, violations = 0, identities = 0;

                // Count sessions
                V2Routes.Route<?> sessionRoute = v2Routes.routeFor(Categories.SESSION);
                if (sessionRoute != null) {
                    Operation<?> op = QueryToOperationTranslator.translate(
                        Categories.SESSION, Queries.listSessionsByPlayer(uuid, 100_000, null));
                    if (op != null) {
                        Object result = executeSync(sessionRoute, op);
                        if (result instanceof Page<?> p) sessions = p.items().size();
                    }
                } else {
                    Backend b = router.backendFor(Categories.SESSION);
                    Page<?> p = b.read(Categories.SESSION, Queries.listSessionsByPlayer(uuid, 100_000, null));
                    sessions = p.items().size();
                }

                // Count violations per session
                V2Routes.Route<?> violationRoute = v2Routes.routeFor(Categories.VIOLATION);
                // simplified: use countViolationsInSession which handles v2/legacy
                // but call it synchronously via the route
                if (violationRoute != null && sessions > 0) {
                    // For v2: we'd need session list to count per-session.
                    // Use a simpler approach: count all violations for the player
                    // directly via the partition.
                    try {
                        Object count = executeSync(violationRoute,
                            new EventStreamOps.CountOp(Categories.VIOLATION, "player_uuid", uuid));
                        if (count instanceof Long l) violations = l.intValue();
                    } catch (Exception e) {
                        violations = 0;
                    }
                } else {
                    Backend b = router.backendFor(Categories.VIOLATION);
                    violations = (int) b.countViolationsInSession(uuid); // approximate
                }

                // Count identities
                V2Routes.Route<?> idRoute = v2Routes.routeFor(Categories.PLAYER_IDENTITY);
                if (idRoute != null) {
                    Operation<?> op = new EntityOps.GetByIdOp<>(Categories.PLAYER_IDENTITY, uuid);
                    Object result = executeSync(idRoute, op);
                    identities = (result instanceof Optional<?> opt && opt.isPresent()) ? 1 : 0;
                } else {
                    Backend b = router.backendFor(Categories.PLAYER_IDENTITY);
                    Page<?> p = b.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(uuid));
                    identities = p.items().size();
                }

                // Deletions — synchronous, no pool submission
                deleteSync(Categories.VIOLATION, Deletes.byPlayer(uuid));
                deleteSync(Categories.SESSION, Deletes.byPlayer(uuid));
                deleteSync(Categories.PLAYER_IDENTITY, Deletes.byPlayer(uuid));
                deleteSync(Categories.SETTING, Deletes.byPlayer(uuid));

                return new DeletionReport(sessions, violations, 0, identities, 0);
            } catch (Exception e) {
                throw new RuntimeException("forgetPlayer failed", e);
            }
        }, reader);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object executeSync(@NotNull V2Routes.Route<?> route, @NotNull Operation<?> op) throws BackendException {
        KindAdapter adapter = route.adapter();
        return adapter.execute(route.storeId(), route.kind(), op);
    }

    private <E> void deleteSync(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) throws BackendException {
        V2Routes.Route<?> route = v2Routes.routeFor(cat);
        if (route != null) {
            Operation<Void> v2Op = DeleteCriteriaTranslator.translate(cat, criteria);
            if (v2Op != null) {
                executeSync(route, v2Op);
                return;
            }
        }
        Backend b = router.backendFor(cat);
        b.delete(cat, criteria);
    }

    @Override
    public CompletionStage<Long> countViolationsInSession(UUID sessionId) {
        V2Routes.Route<?> route = v2Routes.routeFor(Categories.VIOLATION);
        if (route != null) {
            return execute(new EventStreamOps.CountOp(
                Categories.VIOLATION, "session_id", sessionId));
        }
        Backend b = router.backendFor(Categories.VIOLATION);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return b.countViolationsInSession(sessionId);
            } catch (BackendException e) {
                throw new RuntimeException("countViolationsInSession failed", e);
            }
        }, reader);
    }

    @Override
    public CompletionStage<Long> countUniqueChecksInSession(UUID sessionId) {
        V2Routes.Route<?> route = v2Routes.routeFor(Categories.VIOLATION);
        if (route != null) {
            return execute(new EventStreamOps.CountDistinctOp(
                Categories.VIOLATION, "session_id", sessionId, "check_id"));
        }
        Backend b = router.backendFor(Categories.VIOLATION);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return b.countUniqueChecksInSession(sessionId);
            } catch (BackendException e) {
                throw new RuntimeException("countUniqueChecksInSession failed", e);
            }
        }, reader);
    }

    @Override
    public CompletionStage<Long> countSessionsByPlayer(UUID player) {
        V2Routes.Route<?> route = v2Routes.routeFor(Categories.SESSION);
        if (route != null) {
            return execute(new EntityOps.CountByIndexOp(
                Categories.SESSION, "by_player_started", player));
        }
        Backend b = router.backendFor(Categories.SESSION);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return b.countSessionsByPlayer(player);
            } catch (BackendException e) {
                throw new RuntimeException("countSessionsByPlayer failed", e);
            }
        }, reader);
    }

    @Override
    public DataStoreMetrics metrics() {
        return metrics;
    }

    @Override
    public synchronized void flushAndClose(long drainTimeoutMs) {
        if (closed) return;
        closed = true;
        long effective = drainTimeoutMs > 0 ? drainTimeoutMs : shutdownDrainTimeoutMs;
        long leftover = rings.shutdown(effective);

        // Shut the reader pool down BEFORE closing backends — otherwise
        // an in-flight execute() task can race a closed backend (the
        // adapter call would hit a closed Mongo client / SQL pool and
        // either fail or hang). The configured drain timeout applies
        // here too; only fall back to shutdownNow + a brief interrupt
        // grace window after the budget is exhausted.
        reader.shutdown();
        try {
            if (!reader.awaitTermination(effective, TimeUnit.MILLISECONDS)) {
                reader.shutdownNow();
                // Give the interrupt a moment to land. Bounded — if a
                // task ignores interrupt we accept the leak rather than
                // block the plugin's onDisable indefinitely.
                if (!reader.awaitTermination(2, TimeUnit.SECONDS)) {
                    logger.log(java.util.logging.Level.WARNING,
                        "[grim-datastore] reader pool did not terminate after "
                            + effective + "ms drain + 2s interrupt grace;"
                            + " proceeding with backend close (in-flight reads may fail).");
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            reader.shutdownNow();
        }

        for (Backend b : router.allBackends()) {
            try {
                b.flush();
            } catch (BackendException e) {
                logger.log(java.util.logging.Level.WARNING,
                        "[grim-datastore] flush failed for backend " + b.id(), e);
            }
            try {
                b.close();
            } catch (BackendException e) {
                logger.log(java.util.logging.Level.WARNING,
                        "[grim-datastore] close failed for backend " + b.id(), e);
            }
        }
        if (leftover > 0) {
            logger.log(java.util.logging.Level.WARNING,
                    "[grim-datastore] shutdown dropped " + leftover + " unwritten records total");
        }
    }

    /** Read-only accessor used by services (HistoryServiceImpl etc.) for routed reads. */
    public CategoryRouter router() {
        return router;
    }

    /** Reader executor for services that need to hop off the caller thread themselves. */
    public ExecutorService readerExecutor() {
        return reader;
    }
}
