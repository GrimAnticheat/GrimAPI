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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Shared-impl {@link DataStore}. Wires one {@link BoundedMpscQueue} + {@link WriterLoop}
 * per backend, routes submits to the right queue via {@link CategoryRouter}, and runs
 * reads on a small shared executor so the caller thread (e.g. netty or main) doesn't
 * block on backend latency.
 */
@ApiStatus.Internal
public final class DataStoreImpl implements DataStore {

    private final CategoryRouter router;
    private final Map<String, BoundedMpscQueue<WriteEnvelope>> queuesByBackend;
    private final Map<String, WriterLoop> loopsByBackend;
    private final ExecutorService reader;
    private final AggregateDataStoreMetrics metrics;
    private final Logger logger;
    private volatile boolean closed;

    public DataStoreImpl(CategoryRouter router, WritePathConfig writePath, Logger logger) {
        this.router = router;
        this.logger = logger;
        Map<String, BoundedMpscQueue<WriteEnvelope>> queues = new LinkedHashMap<>();
        Map<String, WriterLoop> loops = new LinkedHashMap<>();
        List<BoundedMpscQueue<?>> queueList = new ArrayList<>();
        List<WriterLoop> loopList = new ArrayList<>();
        for (Backend b : router.allBackends()) {
            BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(writePath.queueCapacity());
            WriterLoop loop = new WriterLoop(
                    "grim-datastore-" + b.id(),
                    b,
                    q,
                    writePath.batchSize(),
                    writePath.flushIntervalMs(),
                    writePath.warnRateMs(),
                    logger);
            queues.put(b.id(), q);
            loops.put(b.id(), loop);
            queueList.add(q);
            loopList.add(loop);
        }
        this.queuesByBackend = queues;
        this.loopsByBackend = loops;
        this.metrics = new AggregateDataStoreMetrics(queueList, loopList);
        AtomicInteger readerSeq = new AtomicInteger();
        this.reader = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "grim-datastore-reader-" + readerSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        for (WriterLoop l : loopsByBackend.values()) l.start();
    }

    @Override
    public <R> void submit(Category<R> cat, R record) {
        Backend b = router.backendFor(cat);
        BoundedMpscQueue<WriteEnvelope> q = queuesByBackend.get(b.id());
        q.offer(new WriteEnvelope(cat, record));
    }

    @Override
    public <R> CompletionStage<Page<R>> query(Category<R> cat, Query<R> query) {
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
    public <R> CompletionStage<Void> delete(Category<R> cat, DeleteCriteria criteria) {
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
            int sessions = 0, violations = 0, settings = 0, identities = 0;
            Backend sessionBackend = router.backendFor(Categories.SESSION);
            Backend violationBackend = router.backendFor(Categories.VIOLATION);
            Backend identityBackend = router.backendFor(Categories.PLAYER_IDENTITY);
            Backend settingBackend = router.backendFor(Categories.SETTING);
            try {
                // Count what's there before deletion so we can report it.
                sessions = (int) countSessions(sessionBackend, uuid);
                violations = (int) countViolationsByPlayer(violationBackend, sessionBackend, uuid);
                identities = (int) countIdentities(identityBackend, uuid);
                settings = 0; // settings not tracked-per-player in phase 1 beyond the delete

                violationBackend.delete(Categories.VIOLATION, Deletes.byPlayer(uuid));
                sessionBackend.delete(Categories.SESSION, Deletes.byPlayer(uuid));
                identityBackend.delete(Categories.PLAYER_IDENTITY, Deletes.byPlayer(uuid));
                settingBackend.delete(Categories.SETTING, Deletes.byPlayer(uuid));
            } catch (BackendException e) {
                throw new RuntimeException("forgetPlayer failed", e);
            }
            return new DeletionReport(sessions, violations, settings, identities, 0);
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
        long leftoverTotal = 0;
        for (WriterLoop l : loopsByBackend.values()) {
            leftoverTotal += l.stopAndDrain(drainTimeoutMs);
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
        reader.shutdown();
        try {
            if (!reader.awaitTermination(2, TimeUnit.SECONDS)) reader.shutdownNow();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            reader.shutdownNow();
        }
        if (leftoverTotal > 0) {
            logger.log(java.util.logging.Level.WARNING,
                    "[grim-datastore] shutdown dropped " + leftoverTotal + " unwritten records total");
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

    // visible for debug
    @SuppressWarnings("unused")
    Map<String, WriterLoop> writerLoops() {
        return new HashMap<>(loopsByBackend);
    }
}
