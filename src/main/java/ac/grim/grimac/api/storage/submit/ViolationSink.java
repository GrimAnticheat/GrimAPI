package ac.grim.grimac.api.storage.submit;

import ac.grim.grimac.api.storage.event.ViolationEvent;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Non-blocking violation submit path for the netty thread. Contract: {@link #record}
 * does not block, does not throw on backend failure, and returns a {@link SubmitResult}
 * indicating whether the record was queued or dropped.
 * <p>
 * Hot-path producers populate a pre-allocated {@link ViolationEvent} slot inside
 * {@code configurer}; the ring publishes immediately and the slot is recycled. Do not
 * retain the event past the configurer's return.
 */
@ApiStatus.Experimental
public interface ViolationSink {

    @NotNull SubmitResult record(@NotNull Consumer<ViolationEvent> configurer);
}
