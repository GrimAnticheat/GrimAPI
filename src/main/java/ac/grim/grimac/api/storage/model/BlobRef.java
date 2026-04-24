package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Experimental
public record BlobRef(
        String backendId,
        String key,
        long sizeBytes,
        @Nullable String contentType) {

    public BlobRef {
        if (backendId == null) throw new IllegalArgumentException("backendId");
        if (key == null) throw new IllegalArgumentException("key");
    }
}
