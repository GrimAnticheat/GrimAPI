package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable read-side DTO for one extension-scoped binary record.
 */
@ApiStatus.Experimental
public record ExtensionStorageRecord(
        String categoryId,
        String key,
        byte[] value,
        long createdAtEpochMs,
        long expiresAtEpochMs,
        @Nullable String metadata) {

    public ExtensionStorageRecord {
        if (categoryId == null || categoryId.isBlank()) throw new IllegalArgumentException("categoryId");
        if (key == null) throw new IllegalArgumentException("key");
        if (value == null) throw new IllegalArgumentException("value");
        value = value.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
