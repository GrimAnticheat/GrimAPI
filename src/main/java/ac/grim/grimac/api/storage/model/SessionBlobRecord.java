package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Immutable read-side DTO for a blob attached to a player session.
 */
@ApiStatus.Experimental
public record SessionBlobRecord(
        UUID sessionId,
        UUID playerUuid,
        BlobRef blobRef,
        @Nullable String kind,
        @Nullable String codec,
        long startOffsetMs,
        long durationMs,
        @Nullable String label,
        @Nullable String metadata,
        boolean truncated) {

    public SessionBlobRecord {
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        if (blobRef == null) throw new IllegalArgumentException("blobRef");
    }
}
