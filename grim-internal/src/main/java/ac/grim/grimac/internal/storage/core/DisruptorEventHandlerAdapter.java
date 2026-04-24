package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import com.lmax.disruptor.EventHandler;
import org.jetbrains.annotations.ApiStatus;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bridges the public {@link StorageEventHandler} — the Disruptor-free
 * surface — to LMAX Disruptor's {@link EventHandler}. This is the only
 * place on the write path where {@code com.lmax.disruptor.EventHandler}
 * appears; keeping it behind this adapter lets backend handlers target a
 * relocation-safe interface so packaging (shadow jar, shaded Disruptor)
 * doesn't break them.
 * <p>
 * Rate-limits exception warnings and counts error drops for metrics.
 */
@ApiStatus.Internal
final class DisruptorEventHandlerAdapter<E> implements EventHandler<E> {

    private final String categoryId;
    private final StorageEventHandler<E> delegate;
    private final long warnRateMs;
    private final Logger logger;

    private final AtomicLong droppedOnErrorTotal = new AtomicLong();
    private final AtomicLong handledTotal = new AtomicLong();
    private final AtomicLong batchCommitLatencyMsEma = new AtomicLong();

    private long batchStartNanos;
    private long lastWarnAtMs;
    private long dropsAccumulatedSinceLastWarn;

    DisruptorEventHandlerAdapter(String categoryId,
                                 StorageEventHandler<E> delegate,
                                 long warnRateMs,
                                 Logger logger) {
        this.categoryId = categoryId;
        this.delegate = delegate;
        this.warnRateMs = warnRateMs;
        this.logger = logger;
    }

    @Override
    public void onEvent(E event, long sequence, boolean endOfBatch) {
        if (batchStartNanos == 0L) batchStartNanos = System.nanoTime();
        try {
            delegate.onEvent(event, sequence, endOfBatch);
            handledTotal.incrementAndGet();
        } catch (BackendException | RuntimeException e) {
            droppedOnErrorTotal.incrementAndGet();
            registerDropWarn(e);
        } finally {
            if (endOfBatch) {
                long elapsedMs = (System.nanoTime() - batchStartNanos) / 1_000_000L;
                updateEmaMs(batchCommitLatencyMsEma, elapsedMs);
                batchStartNanos = 0L;
            }
        }
    }

    private synchronized void registerDropWarn(Throwable cause) {
        dropsAccumulatedSinceLastWarn++;
        long now = System.currentTimeMillis();
        if (now - lastWarnAtMs >= warnRateMs) {
            logger.log(Level.WARNING,
                    "[grim-datastore] backend event handler failed for " + categoryId
                            + "; dropped " + dropsAccumulatedSinceLastWarn
                            + " events in the last " + warnRateMs + "ms",
                    cause);
            lastWarnAtMs = now;
            dropsAccumulatedSinceLastWarn = 0L;
        }
    }

    long droppedOnErrorTotal() { return droppedOnErrorTotal.get(); }

    long handledTotal() { return handledTotal.get(); }

    long batchCommitLatencyMsEma() { return batchCommitLatencyMsEma.get(); }

    private static void updateEmaMs(AtomicLong ema, long observedMs) {
        long prev = ema.get();
        long next = prev == 0 ? observedMs : (prev * 7 + observedMs) / 8;
        ema.lazySet(next);
    }
}
