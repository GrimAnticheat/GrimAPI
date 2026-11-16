package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import com.lmax.disruptor.EventHandler;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
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
 * Counts error drops for metrics and rate-limits the warning. While writes keep
 * failing, a warning reports the drops since the previous one, first after
 * {@code warnRateMs} and then twice as far apart each time, up to
 * {@link #MAX_WARN_INTERVAL_MS}. A failure's stack trace is printed the first
 * time it is seen and again at most every {@link #TRACE_INTERVAL_MS}; the
 * other warnings are one line. After {@link #MAX_WARN_INTERVAL_MS} without a
 * failure the interval starts over.
 */
@ApiStatus.Internal
final class DisruptorEventHandlerAdapter<E> implements EventHandler<E> {

    static final long MAX_WARN_INTERVAL_MS = 5L * 60L * 1000L;
    static final long TRACE_INTERVAL_MS = 60L * 60L * 1000L;

    private final String categoryId;
    private final StorageEventHandler<E> delegate;
    private final long warnRateMs;
    private final Logger logger;
    private final LongSupplier clock;

    private final AtomicLong droppedOnErrorTotal = new AtomicLong();
    private final AtomicLong handledTotal = new AtomicLong();
    private final AtomicLong batchCommitLatencyMsEma = new AtomicLong();

    private long batchStartNanos;
    private long lastFailureAtMs = Long.MIN_VALUE;
    private long lastWarnAtMs = Long.MIN_VALUE;
    private long warnIntervalMs;
    private long dropsAccumulatedSinceLastWarn;
    private @Nullable String lastTracedKind;
    private long lastTraceAtMs;

    DisruptorEventHandlerAdapter(String categoryId,
                                 StorageEventHandler<E> delegate,
                                 long warnRateMs,
                                 Logger logger) {
        this(categoryId, delegate, warnRateMs, logger, System::currentTimeMillis);
    }

    DisruptorEventHandlerAdapter(String categoryId,
                                 StorageEventHandler<E> delegate,
                                 long warnRateMs,
                                 Logger logger,
                                 LongSupplier clock) {
        this.categoryId = categoryId;
        this.delegate = delegate;
        this.warnRateMs = warnRateMs;
        this.logger = logger;
        this.clock = clock;
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
        long now = clock.getAsLong();
        long base = Math.max(1L, warnRateMs);
        boolean newEpisode = lastFailureAtMs == Long.MIN_VALUE || now - lastFailureAtMs >= MAX_WARN_INTERVAL_MS;
        if (newEpisode) warnIntervalMs = base;
        lastFailureAtMs = now;
        if (lastWarnAtMs != Long.MIN_VALUE && now - lastWarnAtMs < warnIntervalMs) return;

        String kind = kind(cause);
        String message = "[grim-datastore] backend event handler failed for " + categoryId + "; dropped "
                + dropsAccumulatedSinceLastWarn + " event(s)"
                + (lastWarnAtMs == Long.MIN_VALUE ? "" : " in the last " + (now - lastWarnAtMs) / 1000L + " s");
        if (!kind.equals(lastTracedKind) || now - lastTraceAtMs >= TRACE_INTERVAL_MS) {
            logger.log(Level.WARNING, message, cause);
            lastTracedKind = kind;
            lastTraceAtMs = now;
        } else {
            logger.log(Level.WARNING, message + ": " + kind + " (stack trace logged earlier)");
        }
        lastWarnAtMs = now;
        // The first warning of an episode is followed by one after warnRateMs, then twice as far each time.
        if (!newEpisode) warnIntervalMs = Math.min(MAX_WARN_INTERVAL_MS, warnIntervalMs * 2L);
        dropsAccumulatedSinceLastWarn = 0L;
    }

    /** The root cause's type and message: what makes two failures "the same". */
    private static String kind(Throwable t) {
        Throwable root = t;
        for (int depth = 0; root.getCause() != null && root.getCause() != root && depth < 32; depth++) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getName() + (message == null ? "" : ": " + message);
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
