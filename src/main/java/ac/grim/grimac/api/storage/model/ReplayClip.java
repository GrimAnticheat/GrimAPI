package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Experimental
public record ReplayClip(
        BlobRef ref,
        ReplayRecorderMode mode,
        long startOffsetMs,
        long durationMs,
        @Nullable String label) {

    public ReplayClip {
        if (ref == null) throw new IllegalArgumentException("ref");
        if (mode == null) throw new IllegalArgumentException("mode");
    }
}
