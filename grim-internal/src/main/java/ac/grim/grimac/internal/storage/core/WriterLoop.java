package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single-consumer drain loop feeding a {@link Backend}. Drains a {@link BoundedMpscQueue}
 * in batches, groups by {@link Category}, and dispatches to
 * {@link Backend#writeBatch(Category, List)}. Backend exceptions are logged (rate-limited)
 * and the batch is dropped; no unbounded retry.
 */
@ApiStatus.Internal
public final class WriterLoop {

    private final String threadName;
    private final Backend backend;
    private final BoundedMpscQueue<WriteEnvelope> queue;
    private final int batchSize;
    private final long flushIntervalMs;
    private final long warnRateMs;
    private final Logger logger;

    private volatile Thread worker;
    private volatile boolean shuttingDown;

    private final AtomicLong droppedOnErrorTotal = new AtomicLong();
    private final AtomicLong batchLatencyMsEma = new AtomicLong();
    private long lastWarnAtMs;
    private long dropsAccumulatedSinceLastWarn;

    public WriterLoop(String threadName,
                      Backend backend,
                      BoundedMpscQueue<WriteEnvelope> queue,
                      int batchSize,
                      long flushIntervalMs,
                      long warnRateMs,
                      Logger logger) {
        this.threadName = threadName;
        this.backend = backend;
        this.queue = queue;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.warnRateMs = warnRateMs;
        this.logger = logger;
    }

    public synchronized void start() {
        if (worker != null) throw new IllegalStateException("already started");
        worker = new Thread(this::run, threadName);
        worker.setDaemon(true);
        worker.start();
    }

    private void run() {
        while (!shuttingDown || queue.size() > 0) {
            try {
                // During shutdown, drop the flush timeout so a near-empty queue doesn't stall
                // the drain for a full flushInterval before loop exit.
                long pollMs = shuttingDown ? 50 : flushIntervalMs;
                List<WriteEnvelope> batch = queue.drainUpTo(batchSize, pollMs);
                if (batch.isEmpty()) continue;
                long started = System.nanoTime();
                dispatch(batch);
                long elapsedMs = (System.nanoTime() - started) / 1_000_000;
                updateEmaMs(batchLatencyMsEma, elapsedMs);
            } catch (InterruptedException e) {
                // Only exit on interrupt if we're not in the drain phase; during shutdown
                // the interrupt is the signal to drop remaining work and bail fast.
                Thread.currentThread().interrupt();
                if (shuttingDown) break;
                logger.log(Level.WARNING, "[grim-datastore] writer loop interrupted; stopping", e);
                break;
            } catch (RuntimeException e) {
                logger.log(Level.SEVERE, "[grim-datastore] unexpected exception in writer loop; continuing", e);
            }
        }
    }

    private void dispatch(List<WriteEnvelope> batch) {
        Map<Category<?>, List<Object>> grouped = new LinkedHashMap<>();
        for (WriteEnvelope e : batch) {
            grouped.computeIfAbsent(e.category(), k -> new ArrayList<>()).add(e.record());
        }
        for (Map.Entry<Category<?>, List<Object>> entry : grouped.entrySet()) {
            writeOne(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeOne(Category<?> cat, List<Object> records) {
        try {
            backend.writeBatch((Category) cat, (List) records);
        } catch (BackendException | RuntimeException e) {
            int size = records.size();
            droppedOnErrorTotal.addAndGet(size);
            registerDropWarn(size, e);
        }
    }

    private synchronized void registerDropWarn(int count, Throwable cause) {
        dropsAccumulatedSinceLastWarn += count;
        long now = System.currentTimeMillis();
        if (now - lastWarnAtMs >= warnRateMs) {
            logger.log(Level.WARNING,
                    "[grim-datastore] backend write failed; dropped " + dropsAccumulatedSinceLastWarn
                            + " records in the last " + warnRateMs + "ms",
                    cause);
            lastWarnAtMs = now;
            dropsAccumulatedSinceLastWarn = 0L;
        }
    }

    private static void updateEmaMs(AtomicLong ema, long observedMs) {
        long prev = ema.get();
        long next = prev == 0 ? observedMs : (prev * 7 + observedMs) / 8;
        ema.lazySet(next);
    }

    /**
     * Signal stop + wait up to {@code drainTimeoutMs} for the queue to drain. Any
     * remainder is dropped (counted as overflow). Returns the count dropped this way.
     */
    public long stopAndDrain(long drainTimeoutMs) {
        shuttingDown = true;
        Thread t = worker;
        if (t == null) return 0;
        // Do NOT interrupt immediately — let the loop see shuttingDown and drain queue.size()
        // down to zero on its own. Interrupt only if we time out waiting.
        try {
            t.join(Math.max(0, drainTimeoutMs));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            t.interrupt();
            try {
                t.join(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        long leftover = queue.size();
        if (leftover > 0) {
            logger.log(Level.WARNING,
                    "[grim-datastore] " + threadName + ": shutdown drain timed out with "
                            + leftover + " records unwritten");
        }
        return leftover;
    }

    public long droppedOnErrorTotal() {
        return droppedOnErrorTotal.get();
    }

    public long writeBatchLatencyMsEma() {
        return batchLatencyMsEma.get();
    }
}
