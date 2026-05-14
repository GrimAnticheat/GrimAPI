package ac.grim.grimac.internal.storage.submit;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.submit.SubmitResult;
import ac.grim.grimac.api.storage.submit.ViolationSink;
import ac.grim.grimac.internal.storage.util.UuidV7;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Non-blocking submit into the DataStore's VIOLATION ring. Ring accounting (drop,
 * overflow, error) lives on the {@link ac.grim.grimac.internal.storage.core.RingRegistry};
 * this class just forwards.
 */
@ApiStatus.Internal
public final class ViolationSinkImpl implements ViolationSink {

    private final DataStore store;
    private volatile boolean closed;

    public ViolationSinkImpl(DataStore store) {
        this.store = store;
    }

    @Override
    public @NotNull SubmitResult record(@NotNull Consumer<ViolationEvent> configurer) {
        if (closed) return SubmitResult.DROPPED_SHUTTING_DOWN;
        // Wrap the caller's configurer so we always hand the backend an event
        // with id() populated. Producer-side minting keeps the id round-trippable
        // (cross-backend copy preserves it, the caller can read it back in
        // future SubmitResult shapes) and matches the contract documented on
        // ViolationEvent. A caller can still pre-set event.id() — we only
        // mint when it's null after their configurer runs.
        store.submit(Categories.VIOLATION, event -> {
            configurer.accept(event);
            if (event.id() == null) event.id(UuidV7.next());
        });
        // DataStoreImpl.submit is non-blocking; overflow is reflected in
        // metrics().droppedOnOverflowTotal rather than returned here. We conservatively
        // report QUEUED; a richer SubmitResult with queue-depth probing can follow.
        return SubmitResult.QUEUED;
    }

    public void shutDown() {
        closed = true;
    }
}
