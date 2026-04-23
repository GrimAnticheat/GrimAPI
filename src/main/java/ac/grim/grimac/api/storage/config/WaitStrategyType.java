package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

/**
 * Ring-buffer wait strategy. Read by the host at config-load time and mapped
 * by the internal storage module to the equivalent Disruptor wait strategy
 * of the same name — this enum exists so the public config surface carries
 * no {@code com.lmax.*} types.
 * <p>
 * {@link #BLOCKING} is the default: a condition-variable wait that minimises
 * CPU use at the cost of latency under extreme burst. Pick {@link #YIELDING}
 * or {@link #BUSY_SPIN} only when you have a free core budget and have
 * measured a win over blocking.
 */
@ApiStatus.Experimental
public enum WaitStrategyType {
    BLOCKING,
    TIMEOUT_BLOCKING,
    SLEEPING,
    YIELDING,
    BUSY_SPIN
}
