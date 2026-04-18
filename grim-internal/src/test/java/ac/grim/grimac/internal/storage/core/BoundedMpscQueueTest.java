package ac.grim.grimac.internal.storage.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedMpscQueueTest {

    @Test
    void offerReturnsTrueUntilFullThenFalse() {
        BoundedMpscQueue<Integer> q = new BoundedMpscQueue<>(3);
        assertTrue(q.offer(1));
        assertTrue(q.offer(2));
        assertTrue(q.offer(3));
        assertFalse(q.offer(4));
        assertEquals(3, q.submittedTotal());
        assertEquals(1, q.droppedTotal());
    }

    @Test
    void drainUpToReturnsEmptyListAfterTimeout() throws InterruptedException {
        BoundedMpscQueue<String> q = new BoundedMpscQueue<>(4);
        long start = System.nanoTime();
        List<String> out = q.drainUpTo(10, 50);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(out.isEmpty());
        assertTrue(elapsedMs >= 40, "elapsed=" + elapsedMs);
    }

    @Test
    void drainUpToRespectsBatchSize() throws InterruptedException {
        BoundedMpscQueue<Integer> q = new BoundedMpscQueue<>(100);
        for (int i = 0; i < 50; i++) q.offer(i);
        List<Integer> batch = q.drainUpTo(10, 100);
        assertEquals(10, batch.size());
        assertEquals(40, q.size());
    }

    @Test
    void multipleProducersAllRecordedInCountersEvenOnOverflow() throws InterruptedException {
        BoundedMpscQueue<Integer> q = new BoundedMpscQueue<>(100);
        int producers = 16;
        int perProducer = 1000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(producers);
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        try {
            for (int p = 0; p < producers; p++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perProducer; i++) {
                            q.offer(i);
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        long total = (long) producers * perProducer;
        assertEquals(total, q.submittedTotal() + q.droppedTotal());
        assertTrue(q.droppedTotal() > 0, "expected drops under back-pressure");
    }
}
