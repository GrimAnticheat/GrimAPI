package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class WriterLoopTest {

    private static final Logger LOG = Logger.getLogger("WriterLoopTest");

    @Test
    void drainsBatchesToBackendUntilEmpty() throws Exception {
        InMemoryBackend backend = new InMemoryBackend();
        BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(2048);
        WriterLoop loop = new WriterLoop("wl-test", backend, q, 64, 20, 60_000, LOG);
        loop.start();
        try {
            UUID session = UUID.randomUUID();
            UUID player = UUID.randomUUID();
            for (int i = 0; i < 500; i++) {
                q.offer(new WriteEnvelope(Categories.VIOLATION, violation(session, player, i)));
            }
            awaitUntilZero(q, 3_000);
            assertEquals(500, backend.countViolationsInSession(session));
        } finally {
            loop.stopAndDrain(2_000);
        }
    }

    @Test
    void burstySubmitsFromManyThreadsEventuallyAllPersist() throws Exception {
        InMemoryBackend backend = new InMemoryBackend();
        BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(4096);
        WriterLoop loop = new WriterLoop("wl-burst", backend, q, 128, 10, 60_000, LOG);
        loop.start();
        int producers = 8;
        int perProducer = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers);
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        try {
            for (int p = 0; p < producers; p++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perProducer; i++) {
                            while (!q.offer(new WriteEnvelope(Categories.VIOLATION, violation(session, player, i)))) {
                                Thread.yield();
                            }
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
            awaitUntilZero(q, 5_000);
            assertEquals((long) producers * perProducer, backend.countViolationsInSession(session));
        } finally {
            pool.shutdownNow();
            loop.stopAndDrain(3_000);
        }
    }

    @Test
    void overflowDropsAreCountedButDontBlock() {
        InMemoryBackend backend = new InMemoryBackend();
        BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(8);
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        for (int i = 0; i < 100; i++) {
            q.offer(new WriteEnvelope(Categories.VIOLATION, violation(session, player, i)));
        }
        assertEquals(8, q.submittedTotal());
        assertEquals(92, q.droppedTotal());
    }

    @Test
    void backendFailuresIncrementErrorDropCounterAndContinue() throws Exception {
        FailOnceBackend backend = new FailOnceBackend();
        BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(2048);
        WriterLoop loop = new WriterLoop("wl-fail", backend, q, 32, 10, 60_000, LOG);
        loop.start();
        try {
            UUID session = UUID.randomUUID();
            UUID player = UUID.randomUUID();
            for (int i = 0; i < 200; i++) {
                q.offer(new WriteEnvelope(Categories.VIOLATION, violation(session, player, i)));
            }
            awaitUntilZero(q, 5_000);
            assertTrue(loop.droppedOnErrorTotal() > 0, "expected error drops, got 0");
            assertTrue(backend.calls.get() > 1, "expected backend to be called more than once");
        } finally {
            loop.stopAndDrain(2_000);
        }
    }

    @Test
    void shutdownWithQueueStillPopulatedDrainsWithinTimeout() throws Exception {
        InMemoryBackend backend = new InMemoryBackend();
        BoundedMpscQueue<WriteEnvelope> q = new BoundedMpscQueue<>(8192);
        WriterLoop loop = new WriterLoop("wl-shutdown", backend, q, 256, 5, 60_000, LOG);
        loop.start();
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        for (int i = 0; i < 4000; i++) {
            q.offer(new WriteEnvelope(Categories.VIOLATION, violation(session, player, i)));
        }
        long left = loop.stopAndDrain(5_000);
        assertEquals(0, left, "expected full drain within 5s for 4000 records");
        assertEquals(4000, backend.countViolationsInSession(session));
    }

    private static void awaitUntilZero(BoundedMpscQueue<?> q, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (q.size() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    private static ViolationRecord violation(UUID sessionId, UUID player, long time) {
        return new ViolationRecord(0, sessionId, player, 1, 1.0, time, "v", VerboseFormat.TEXT);
    }

    /** Fails once, then delegates. Exercises the error-drop + continue path. */
    private static final class FailOnceBackend implements Backend {
        final InMemoryBackend delegate = new InMemoryBackend();
        final AtomicInteger calls = new AtomicInteger();

        @Override public String id() { return "fail-once"; }
        @Override public ApiVersion getApiVersion() { return ApiVersion.CURRENT; }
        @Override public EnumSet<Capability> capabilities() { return delegate.capabilities(); }
        @Override public Set<Category<?>> supportedCategories() { return delegate.supportedCategories(); }
        @Override public void init(BackendContext ctx) {}
        @Override public void flush() {}
        @Override public void close() { delegate.close(); }

        @Override
        public <R> void writeBatch(Category<R> cat, List<R> records) throws BackendException {
            if (calls.incrementAndGet() == 1) throw new BackendException("simulated failure");
            delegate.writeBatch(cat, records);
        }

        @Override
        public <R> Page<R> read(Category<R> cat, Query<R> query) throws BackendException {
            return delegate.read(cat, query);
        }

        @Override
        public <R> void delete(Category<R> cat, DeleteCriteria criteria) throws BackendException {
            delegate.delete(cat, criteria);
        }

        @Override
        public long countViolationsInSession(UUID sessionId) throws BackendException {
            return delegate.countViolationsInSession(sessionId);
        }
    }
}
