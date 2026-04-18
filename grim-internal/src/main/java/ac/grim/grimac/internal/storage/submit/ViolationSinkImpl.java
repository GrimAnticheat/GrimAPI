package ac.grim.grimac.internal.storage.submit;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.submit.SubmitResult;
import ac.grim.grimac.api.storage.submit.ViolationSink;
import org.jetbrains.annotations.ApiStatus;

/**
 * Non-blocking submit into the DataStore's VIOLATION queue. All drop + overflow
 * accounting lives in {@link ac.grim.grimac.internal.storage.core.BoundedMpscQueue};
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
    public SubmitResult record(ViolationRecord record) {
        if (closed) return SubmitResult.DROPPED_SHUTTING_DOWN;
        // DataStoreImpl.submit is non-blocking; overflow is reflected in
        // metrics().droppedOnOverflowTotal rather than returned here. We conservatively
        // report QUEUED; a richer SubmitResult with queue-depth probing can follow.
        store.submit(Categories.VIOLATION, record);
        return SubmitResult.QUEUED;
    }

    public void shutDown() {
        closed = true;
    }
}
