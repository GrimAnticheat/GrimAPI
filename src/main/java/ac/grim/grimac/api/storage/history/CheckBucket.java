package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A time-bucket of violations within a session, aggregated by check. Buckets are
 * emitted at a fixed {@code bucketSizeMs} (carried on {@link SessionDetail}) starting
 * from session start; {@code bucketStartOffsetMs} is the offset of the bucket's
 * leading edge from session start.
 */
@ApiStatus.Experimental
public record CheckBucket(
        long bucketStartOffsetMs,
        @NotNull List<CheckCount> checks) {

    public CheckBucket {
        checks = checks == null ? List.of() : List.copyOf(checks);
    }
}
