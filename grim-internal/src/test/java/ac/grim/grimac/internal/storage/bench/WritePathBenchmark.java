package ac.grim.grimac.internal.storage.bench;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WaitStrategyType;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone microbenchmark for the Disruptor write path. Not a JUnit test — invoke
 * via {@code main()} or a Gradle JavaExec task. Reports submit throughput and
 * approximate allocation-free-ness by leaning on a zero-capture lambda at the
 * producer site.
 * <p>
 * This does NOT stand in for a full JMH harness. JMH requires a dedicated source
 * set + bytecode processors and the numbers from a simple {@code System.nanoTime}
 * loop are only meaningful after warmup — treat output as an order-of-magnitude
 * sanity check, not a publishable benchmark.
 */
public final class WritePathBenchmark {

    private static final int WARMUP_EVENTS = 200_000;
    private static final int MEASURED_EVENTS_PER_THREAD = 1_000_000;
    private static final int[] PRODUCER_THREAD_COUNTS = new int[]{1, 2, 4, 8};

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("bench");
        logger.setLevel(Level.WARNING);

        for (int threads : PRODUCER_THREAD_COUNTS) {
            runScenario(threads, logger);
            System.out.println();
        }
    }

    private static void runScenario(int producerThreads, Logger logger) throws Exception {
        InMemoryBackend backend = new InMemoryBackend();
        Map<Category<?>, Backend> routing = Map.of(
                Categories.VIOLATION, backend,
                Categories.SESSION, backend,
                Categories.PLAYER_IDENTITY, backend,
                Categories.SETTING, backend);
        WritePathConfig cfg = new WritePathConfig(65536, 256, 1000L, 10_000L, 5_000L,
                WaitStrategyType.BLOCKING);
        DataStoreImpl store = new DataStoreImpl(new CategoryRouter(routing), cfg, logger);
        store.start();
        try {
            // Warmup.
            ExecutorService pool = Executors.newFixedThreadPool(producerThreads);
            runProducers(pool, store, producerThreads, WARMUP_EVENTS / producerThreads);
            // Measured.
            long started = System.nanoTime();
            runProducers(pool, store, producerThreads, MEASURED_EVENTS_PER_THREAD);
            long submitElapsedNs = System.nanoTime() - started;
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);

            // Wait for consumer to drain — separate from submit time.
            long drainStart = System.nanoTime();
            while (store.metrics().queuedCount() > 0) {
                Thread.sleep(1);
            }
            long drainElapsedNs = System.nanoTime() - drainStart;

            long totalEvents = (long) producerThreads * MEASURED_EVENTS_PER_THREAD;
            double nsPerSubmit = (double) submitElapsedNs / totalEvents;
            double eventsPerSec = 1_000_000_000.0 / nsPerSubmit;
            System.out.printf(
                    "producers=%-2d  submit: %.1f ns/op, %.2fM events/sec, drain: %d ms, dropped=%d%n",
                    producerThreads, nsPerSubmit, eventsPerSec / 1_000_000.0,
                    drainElapsedNs / 1_000_000L,
                    store.metrics().droppedOnOverflowTotal());
        } finally {
            store.flushAndClose(5_000);
        }
    }

    private static void runProducers(ExecutorService pool, DataStoreImpl store,
                                     int producers, int perThread) throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers);
        LongAdder spins = new LongAdder();
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        for (int i = 0; i < producers; i++) {
            pool.submit(() -> {
                try {
                    go.await();
                    for (int n = 0; n < perThread; n++) {
                        final long t = n;
                        store.submit(Categories.VIOLATION, e -> e
                                .sessionId(session).playerUuid(player).checkId(1).vl(1.0)
                                .occurredEpochMs(t).verbose("v").verboseFormat(VerboseFormat.TEXT));
                    }
                    // Unused, retained to minimise future diff if back-pressure retry is added.
                    if (spins.longValue() < 0) throw new AssertionError();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        go.countDown();
        done.await(5, TimeUnit.MINUTES);
    }

    private WritePathBenchmark() {}
}
