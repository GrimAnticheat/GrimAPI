package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.config.WaitStrategyType;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Maps the operator-facing {@link WaitStrategyType} enum to a Disruptor
 * {@link WaitStrategy}. Kept internal so the Disruptor types never cross into
 * Layer 1 config.
 */
@ApiStatus.Internal
final class WaitStrategies {

    private WaitStrategies() {}

    static @NotNull WaitStrategy resolve(@NotNull WaitStrategyType type, long timeoutBlockingMs) {
        return switch (type) {
            case BLOCKING -> new BlockingWaitStrategy();
            case TIMEOUT_BLOCKING -> new TimeoutBlockingWaitStrategy(
                    Math.max(1L, timeoutBlockingMs), TimeUnit.MILLISECONDS);
            case SLEEPING -> new SleepingWaitStrategy();
            case YIELDING -> new YieldingWaitStrategy();
            case BUSY_SPIN -> new BusySpinWaitStrategy();
        };
    }
}
