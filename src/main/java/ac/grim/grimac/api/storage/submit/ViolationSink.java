package ac.grim.grimac.api.storage.submit;

import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.jetbrains.annotations.ApiStatus;

/**
 * Non-blocking violation submit path for the netty thread. Contract: {@link #record}
 * does not block, does not throw on backend failure, and returns a {@link SubmitResult}
 * indicating whether the record was queued or dropped.
 */
@ApiStatus.Experimental
public interface ViolationSink {

    SubmitResult record(ViolationRecord record);
}
