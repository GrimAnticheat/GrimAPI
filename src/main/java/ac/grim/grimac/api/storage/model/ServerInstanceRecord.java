package ac.grim.grimac.api.storage.model;

import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Indexed;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * One row per live JVM running Grim against a shared storage backend.
 * Underpins multi-server crash recovery — see
 * {@code .docs/storage-redesign/07-crash-recovery.md}.
 * <p>
 * {@code instanceId} is opaque (fresh UUID at every JVM start); operators
 * never see or set it. {@code serverName} is the operator-configured
 * display label — display-only, freely renamable, non-unique. Two
 * servers can both be named "survival" without ambiguity since they'll
 * have different instance ids.
 * <p>
 * {@code lastHeartbeatEpochMs} is updated on a scheduled heartbeat loop
 * (default 30s); backends with native TTL support (Mongo, Redis) auto-evict
 * rows whose heartbeat hasn't been refreshed within the configured
 * eviction window (default 5× heartbeat = 150s). SQL backends run an
 * equivalent scheduled DELETE.
 * <p>
 * {@code drainMode} is a graceful-shutdown hint — drained instances stop
 * heartbeating and let their TTL run out naturally, allowing other
 * instances' sweepers to mark their sessions closed.
 */
@ApiStatus.Experimental
@Persistent
public record ServerInstanceRecord(
        @Id                                          @NotNull UUID instanceId,
        @Indexed @Name("server_name")                @NotNull String serverName,
        @Value @Name("started_at")                   long startedEpochMs,
        @Value @Name("last_heartbeat")               long lastHeartbeatEpochMs,
        @Value @Nullable                             String hostname,
        @Value @Name("grim_version") @Nullable       String grimVersion,
        @Value @Name("drain_mode")                   boolean drainMode) {

    public ServerInstanceRecord {
        if (instanceId == null) throw new IllegalArgumentException("instanceId");
        if (serverName == null) throw new IllegalArgumentException("serverName");
    }
}
