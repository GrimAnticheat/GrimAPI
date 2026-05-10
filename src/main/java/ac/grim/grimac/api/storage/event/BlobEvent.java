package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.BlobRef;
import ac.grim.grimac.api.storage.model.SessionBlobRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Mutable write-path slot for a blob attached to a player session.
 *
 * <p>The bytes themselves live in {@code BlobStore}. This event only carries
 * the small indexable metadata needed to attach that blob to a session.
 */
@ApiStatus.Experimental
public final class BlobEvent {

    private BlobRef blobRef;
    private UUID sessionId;
    private UUID playerUuid;
    private @Nullable String kind;
    private @Nullable String codec;
    private long startOffsetMs;
    private long durationMs;
    private @Nullable String label;
    private @Nullable String metadata;
    private boolean truncated;

    public @NotNull BlobRef blobRef() { return blobRef; }
    public @NotNull BlobEvent blobRef(@NotNull BlobRef v) { this.blobRef = v; return this; }

    public @NotNull UUID sessionId() { return sessionId; }
    public @NotNull BlobEvent sessionId(@NotNull UUID v) { this.sessionId = v; return this; }

    public @NotNull UUID playerUuid() { return playerUuid; }
    public @NotNull BlobEvent playerUuid(@NotNull UUID v) { this.playerUuid = v; return this; }

    public @Nullable String kind() { return kind; }
    public @NotNull BlobEvent kind(@Nullable String v) { this.kind = v; return this; }

    public @Nullable String codec() { return codec; }
    public @NotNull BlobEvent codec(@Nullable String v) { this.codec = v; return this; }

    public long startOffsetMs() { return startOffsetMs; }
    public @NotNull BlobEvent startOffsetMs(long v) { this.startOffsetMs = v; return this; }

    public long durationMs() { return durationMs; }
    public @NotNull BlobEvent durationMs(long v) { this.durationMs = v; return this; }

    public @Nullable String label() { return label; }
    public @NotNull BlobEvent label(@Nullable String v) { this.label = v; return this; }

    public @Nullable String metadata() { return metadata; }
    public @NotNull BlobEvent metadata(@Nullable String v) { this.metadata = v; return this; }

    public boolean truncated() { return truncated; }
    public @NotNull BlobEvent truncated(boolean v) { this.truncated = v; return this; }

    public @NotNull SessionBlobRecord toRecord() {
        return new SessionBlobRecord(
                sessionId,
                playerUuid,
                blobRef,
                kind,
                codec,
                startOffsetMs,
                durationMs,
                label,
                metadata,
                truncated);
    }

    public void reset() {
        blobRef = null;
        sessionId = null;
        playerUuid = null;
        kind = null;
        codec = null;
        startOffsetMs = 0L;
        durationMs = 0L;
        label = null;
        metadata = null;
        truncated = false;
    }
}
