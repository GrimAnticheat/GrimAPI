package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

/**
 * Ring-buffer wait strategy, selected at YAML-load time. Layer 2 maps this to the
 * shaded Disruptor wait strategy of the same name; keeping the enum at Layer 1
 * avoids leaking {@code com.lmax.*} types into the public config surface.
 * <p>
 * {@link #BLOCKING} is the default: a condition-variable wait that minimises CPU
 * use at the cost of latency under extreme burst. Pick {@link #YIELDING} or
 * {@link #BUSY_SPIN} only when you have a free core budget and have measured a
 * win over blocking.
 */
@ApiStatus.Experimental
public enum WaitStrategyType {
    BLOCKING,
    TIMEOUT_BLOCKING,
    SLEEPING,
    YIELDING,
    BUSY_SPIN
}
