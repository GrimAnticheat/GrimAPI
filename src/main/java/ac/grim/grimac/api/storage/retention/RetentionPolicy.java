package ac.grim.grimac.api.storage.retention;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public interface RetentionPolicy<R> {

    boolean shouldExpire(R record, long nowEpochMs);

    RetentionAction onExpire(R record);
}
