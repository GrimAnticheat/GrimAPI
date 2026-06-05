package ac.grim.grimac.api.storage.model;

import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Indexed;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.PreserveOnNonNull;
import ac.grim.grimac.api.storage.codec.Sentinel;
import ac.grim.grimac.api.storage.codec.Transient;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.UUID;

/**
 * Immutable read-side DTO for one session.
 * <p>
 * {@code clientVersion} is a PacketEvents protocol version number (the same int
 * {@code ClientVersion#getProtocolVersion()} returns). Storing a PVN — not a
 * display string — keeps the API stable across display-name drift and lets
 * renderers hand the int to PE for formatting. {@code -1} means unresolved.
 * <p>
 * {@code closedAtEpochMs} uses {@code 0L} as the "still open" sentinel.
 * Set to a real epoch-ms timestamp by the disconnect path on graceful
 * close, or by the startup crash sweep (using {@code last_activity} as
 * best estimate) for sessions whose connections went away without firing
 * the disconnect path. Renderers should use {@link #isClosed()} rather
 * than comparing to 0 directly. No real session can plausibly close at
 * epoch 0 (1970-01-01), so the sentinel is unambiguous in practice.
 * <p>
 * {@code sessionBlobs} is currently a placeholder for the future
 * recorder/session blob feature and is never persisted — marked
 * {@link Transient @Transient} so the codec skips it on encode/decode.
 * Decode always reconstructs it as an empty list.
 * <p>
 * {@code startupId} identifies the durable {@link ServerStartupRecord}
 * row for the JVM boot that owns this session. It is the only owner key
 * V2 needs to persist on every session; server name/version metadata
 * is resolved through the startup row when rendering history.
 * <p>
 * {@code serverName}, {@code grimVersion}, {@code serverVersionString},
 * and {@code instanceId} remain as nullable legacy fallback fields for
 * direct pre-V2 backends and migrated rows. New V2 writers leave them
 * null; Mongo/Redis omit those nulls and SQL stores them as null columns.
 */
@ApiStatus.Experimental
@Persistent
public record SessionRecord(
        @Id                                                    UUID sessionId,
        @Indexed                                               UUID playerUuid,
        @Value @Nullable                                       String serverName,
        // The next three fields use @Name to publish short Mongo column
        // names that match the legacy MongoBackend and the SQL schemas
        // (started_at / last_activity / closed_at). Without @Name the
        // codec auto-snake-cases to started_epoch_ms etc., which breaks
        // IndexSpec("-started_at") and the markCrashedSessions sweep.
        @Value @Name("started_at")                             long startedEpochMs,
        @Value @Name("last_activity")                          long lastActivityEpochMs,
        // @Sentinel(OPEN=0L): the primitive-long counterpart to
        // @PreserveOnNonNull. Heartbeats send 0L; once a close event
        // stamps a real timestamp, subsequent heartbeats' 0L can't
        // re-open the session. See MergeMode.PRESERVE_ON_NON_SENTINEL.
        @Value @Name("closed_at") @Sentinel(OPEN)              long closedAtEpochMs,
        @Value @Nullable                                       String grimVersion,
        @Value @Nullable                                       String clientBrand,
        @Value @Name("client_version_pvn")                     int clientVersion,
        @Value @Name("server_version") @Nullable               String serverVersionString,
        @Value @Name("instance_id") @Nullable @PreserveOnNonNull UUID instanceId,
        @Indexed @Name("startup_id") @Nullable @PreserveOnNonNull UUID startupId,
        @Transient                                             List<SessionBlobRecord> sessionBlobs) {

    /** Sentinel value indicating an unclosed (live or crashed-but-not-swept) session. */
    public static final long OPEN = 0L;

    public SessionRecord {
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        sessionBlobs = sessionBlobs == null ? List.of() : List.copyOf(sessionBlobs);
    }

    public SessionRecord(
            UUID sessionId,
            UUID playerUuid,
            String serverName,
            long startedEpochMs,
            long lastActivityEpochMs,
            @org.jetbrains.annotations.Nullable Long closedAtEpochMs,
            String grimVersion,
            String clientBrand,
            int clientVersion,
            String serverVersionString,
            List<SessionBlobRecord> sessionBlobs) {
        this(sessionId, playerUuid, serverName, startedEpochMs, lastActivityEpochMs,
                closedAtEpochMs == null ? OPEN : closedAtEpochMs,
                grimVersion, clientBrand, clientVersion, serverVersionString, null, null, sessionBlobs);
    }

    public SessionRecord(
            UUID sessionId,
            UUID playerUuid,
            String serverName,
            long startedEpochMs,
            long lastActivityEpochMs,
            long closedAtEpochMs,
            String grimVersion,
            String clientBrand,
            int clientVersion,
            String serverVersionString,
            UUID instanceId,
            List<SessionBlobRecord> sessionBlobs) {
        this(sessionId, playerUuid, serverName, startedEpochMs, lastActivityEpochMs, closedAtEpochMs,
                grimVersion, clientBrand, clientVersion, serverVersionString, instanceId, null, sessionBlobs);
    }

    /** {@code true} when {@code closedAtEpochMs} carries a real close timestamp ({@code != 0L}). */
    public boolean isClosed() {
        return closedAtEpochMs != OPEN;
    }
}
