package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.ExtensionStorageRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable write-path slot for extension-defined binary records.
 *
 * <p>Extensions own the payload codec. The datastore stores and indexes the
 * scoped category id, key, creation timestamp, optional expiry timestamp, and
 * payload bytes. Event instances are preallocated by the category ring and
 * reused; callers must not retain a reference after submit returns.
 */
@ApiStatus.Experimental
public final class ExtensionStorageEvent {
    private @Nullable String key;
    private byte @Nullable [] value;
    private long createdAtEpochMs;
    private long expiresAtEpochMs;
    private @Nullable String metadata;

    public @NotNull String key() { return key; }
    public @NotNull ExtensionStorageEvent key(@NotNull String v) { this.key = v; return this; }

    public byte @NotNull [] value() { return value; }
    public @NotNull ExtensionStorageEvent value(byte @NotNull [] v) { this.value = v; return this; }

    public long createdAtEpochMs() { return createdAtEpochMs; }
    public @NotNull ExtensionStorageEvent createdAtEpochMs(long v) { this.createdAtEpochMs = v; return this; }

    /** Zero means "no explicit expiry". */
    public long expiresAtEpochMs() { return expiresAtEpochMs; }
    public @NotNull ExtensionStorageEvent expiresAtEpochMs(long v) { this.expiresAtEpochMs = v; return this; }

    public @Nullable String metadata() { return metadata; }
    public @NotNull ExtensionStorageEvent metadata(@Nullable String v) { this.metadata = v; return this; }

    public @NotNull ExtensionStorageRecord toRecord(@NotNull String categoryId) {
        return new ExtensionStorageRecord(categoryId, key, value, createdAtEpochMs, expiresAtEpochMs, metadata);
    }

    public void reset() {
        key = null;
        value = null;
        createdAtEpochMs = 0L;
        expiresAtEpochMs = 0L;
        metadata = null;
    }
}
