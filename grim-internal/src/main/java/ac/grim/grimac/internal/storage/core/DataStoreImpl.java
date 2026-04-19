package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;

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

    /**
     * Wires one ring per routed category. Must be called after {@link CategoryRouter}
     * is populated and the backends have been {@code init}ed.
     */
    public void start() {
        try {
            for (Category<?> cat : router.routedCategories()) registerRingFor(cat);
        } catch (BackendException e) {
            throw new RuntimeException("failed to start DataStore ring", e);
        }
    }

    private <E> void registerRingFor(Category<E> cat) throws BackendException {
        Backend b = router.backendFor(cat);
        rings.register(cat, b);
    }

    @Override
    public <E> void submit(Category<E> cat, Consumer<E> configurer) {
        if (closed) return;
        rings.submit(cat, configurer);
    }

    @Override
    public <R> CompletionStage<Page<R>> query(Category<?> cat, Query<R> query) {
        Backend b = router.backendFor(cat);
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

    @Override
    public <E> CompletionStage<Void> delete(Category<E> cat, DeleteCriteria criteria) {
        Backend b = router.backendFor(cat);
        return CompletableFuture.runAsync(() -> {
            try {
                b.delete(cat, criteria);
            } catch (BackendException e) {
                throw new RuntimeException("backend delete failed", e);
            }
        }, reader);
    }

    @Override
    public CompletionStage<DeletionReport> forgetPlayer(UUID uuid) {
        return CompletableFuture.supplyAsync(() -> {
            int sessions, violations, identities;
            Backend sessionBackend = router.backendFor(Categories.SESSION);
            Backend violationBackend = router.backendFor(Categories.VIOLATION);
            Backend identityBackend = router.backendFor(Categories.PLAYER_IDENTITY);
            Backend settingBackend = router.backendFor(Categories.SETTING);
            try {
                // Count what's there before deletion so we can report it.
                sessions = (int) countSessions(sessionBackend, uuid);
                violations = (int) countViolationsByPlayer(violationBackend, sessionBackend, uuid);
                identities = (int) countIdentities(identityBackend, uuid);

                violationBackend.delete(Categories.VIOLATION, Deletes.byPlayer(uuid));
                sessionBackend.delete(Categories.SESSION, Deletes.byPlayer(uuid));
                identityBackend.delete(Categories.PLAYER_IDENTITY, Deletes.byPlayer(uuid));
                settingBackend.delete(Categories.SETTING, Deletes.byPlayer(uuid));
            } catch (BackendException e) {
                throw new RuntimeException("forgetPlayer failed", e);
            }
            return new DeletionReport(sessions, violations, 0, identities, 0);
        }, reader);
    }

    private long countSessions(Backend b, UUID uuid) throws BackendException {
        Page<?> p = b.read(Categories.SESSION, Queries.listSessionsByPlayer(uuid, 100_000, null));
        return p.items().size();
    }

    private long countViolationsByPlayer(Backend violationBackend, Backend sessionBackend, UUID uuid) throws BackendException {
        Page<?> sessions = sessionBackend.read(Categories.SESSION, Queries.listSessionsByPlayer(uuid, 100_000, null));
        long total = 0;
        for (Object o : sessions.items()) {
            UUID sid = ((ac.grim.grimac.api.storage.model.SessionRecord) o).sessionId();
            total += violationBackend.countViolationsInSession(sid);
        }
        return total;
    }

    private long countIdentities(Backend b, UUID uuid) throws BackendException {
        Page<?> p = b.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(uuid));
        return p.items().size();
    }

    @Override
    public CompletionStage<Long> countViolationsInSession(UUID sessionId) {
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
    public DataStoreMetrics metrics() {
        return metrics;
    }

    @Override
    public synchronized void flushAndClose(long drainTimeoutMs) {
        if (closed) return;
        closed = true;
        long effective = drainTimeoutMs > 0 ? drainTimeoutMs : shutdownDrainTimeoutMs;
        long leftover = rings.shutdown(effective);
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
        reader.shutdown();
        try {
            if (!reader.awaitTermination(2, TimeUnit.SECONDS)) reader.shutdownNow();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            reader.shutdownNow();
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
