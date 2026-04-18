package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record WritePathConfig(
        int queueCapacity,
        int batchSize,
        long flushIntervalMs,
        long warnRateMs,
        long shutdownDrainTimeoutMs) {

    public static WritePathConfig defaults() {
        return new WritePathConfig(16384, 256, 1000L, 10_000L, 5000L);
    }
}
