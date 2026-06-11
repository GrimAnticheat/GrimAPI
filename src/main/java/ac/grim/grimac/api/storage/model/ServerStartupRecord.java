package ac.grim.grimac.api.storage.model;

import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Indexed;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.PreserveOnNonNull;
import ac.grim.grimac.api.storage.codec.Sentinel;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Durable metadata for one Grim JVM startup. Sessions store only
 * {@code startupId}; history display resolves server name/version through
 * this record, while crash recovery uses {@code instanceId} and
 * {@code lastHeartbeatEpochMs}.
 */
@ApiStatus.Experimental
@Persistent
public record ServerStartupRecord(
        @Id                                                    @NotNull UUID startupId,
        @Indexed @Name("instance_id")                         @NotNull UUID instanceId,
        @Indexed @Name("server_name")                         @NotNull String serverName,
        @Value @Name("grim_version") @Nullable                String grimVersion,
        @Value @Name("server_version") @Nullable              String serverVersionString,
        @Value @Nullable                                      String hostname,
        @Value @Name("started_at")                            long startedEpochMs,
        @Value @Name("last_heartbeat")                        long lastHeartbeatEpochMs,
        @Value @Name("closed_at") @Sentinel(OPEN)             long closedAtEpochMs,
        @Value @Name("close_reason") @Nullable @PreserveOnNonNull String closeReason,
        @Value @Name("verbose_manifest") @Nullable byte[] verboseManifest) {

    public static final long OPEN = 0L;

    public ServerStartupRecord {
        if (startupId == null) throw new IllegalArgumentException("startupId");
        if (instanceId == null) throw new IllegalArgumentException("instanceId");
        if (serverName == null) throw new IllegalArgumentException("serverName");
    }

    public boolean isClosed() {
        return closedAtEpochMs != OPEN;
    }
}
