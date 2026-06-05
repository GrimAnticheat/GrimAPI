package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Write-path tunables, applied per category ring.
 * <p>
 * {@code queueCapacity} <strong>must be a positive power of two</strong> — a
 * ring-buffer requirement. The host's config loader should validate this at
 * load time with a helpful error; the record's canonical constructor also
 * enforces it as a last line of defence.
 * <p>
 * {@code batchSize} caps how many events a single handler invocation commits at
 * once; {@code endOfBatch} hinting from the processor drives most commits in
 * practice, so this is a ceiling, not a target.
 */
@ApiStatus.Experimental
public record WritePathConfig(
        int queueCapacity,
        int batchSize,
        long flushIntervalMs,
        long warnRateMs,
        long shutdownDrainTimeoutMs,
        @NotNull WaitStrategyType waitStrategy) {

    public WritePathConfig {
        if (queueCapacity <= 0 || Integer.bitCount(queueCapacity) != 1) {
            throw new IllegalArgumentException(
                    "queueCapacity must be a positive power of two (got " + queueCapacity + ")");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0 (got " + batchSize + ")");
        }
        if (flushIntervalMs < 0) {
            throw new IllegalArgumentException("flushIntervalMs must be >= 0 (got " + flushIntervalMs + ")");
        }
        if (warnRateMs < 0) {
            throw new IllegalArgumentException("warnRateMs must be >= 0 (got " + warnRateMs + ")");
        }
        if (shutdownDrainTimeoutMs < 0) {
            // Negative would propagate to awaitTermination(...) as undefined
            // behavior (and to Disruptor's ring shutdown as 'infinite'),
            // either of which is a foot-gun. 0 = 'no drain, exit immediately'
            // is the legitimate zero case.
            throw new IllegalArgumentException(
                    "shutdownDrainTimeoutMs must be >= 0 (got " + shutdownDrainTimeoutMs + ")");
        }
        if (waitStrategy == null) waitStrategy = WaitStrategyType.BLOCKING;
    }

    public static WritePathConfig defaults() {
        return new WritePathConfig(16384, 256, 1000L, 10_000L, 5000L, WaitStrategyType.BLOCKING);
    }
}
