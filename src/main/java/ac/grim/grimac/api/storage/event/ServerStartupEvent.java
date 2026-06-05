package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Mutable write-path slot for one Grim JVM startup. The row is durable
 * history metadata and also carries the live heartbeat timestamp used by
 * crash recovery. Immutable read counterpart is {@link ServerStartupRecord}.
 */
@ApiStatus.Experimental
public final class ServerStartupEvent {

    private @Nullable UUID startupId;
    private @Nullable UUID instanceId;
    private @Nullable String serverName;
    private @Nullable String grimVersion;
    private @Nullable String serverVersionString;
    private @Nullable String hostname;
    private long startedEpochMs;
    private long lastHeartbeatEpochMs;
    private long closedAtEpochMs;
    private @Nullable String closeReason;
    private @Nullable byte[] verboseManifest;

    public @Nullable UUID startupId() { return startupId; }
    public @NotNull ServerStartupEvent startupId(@NotNull UUID v) { this.startupId = v; return this; }

    public @Nullable UUID instanceId() { return instanceId; }
    public @NotNull ServerStartupEvent instanceId(@NotNull UUID v) { this.instanceId = v; return this; }

    public @Nullable String serverName() { return serverName; }
    public @NotNull ServerStartupEvent serverName(@NotNull String v) { this.serverName = v; return this; }

    public @Nullable String grimVersion() { return grimVersion; }
    public @NotNull ServerStartupEvent grimVersion(@Nullable String v) { this.grimVersion = v; return this; }

    public @Nullable String serverVersionString() { return serverVersionString; }
    public @NotNull ServerStartupEvent serverVersionString(@Nullable String v) {
        this.serverVersionString = v;
        return this;
    }

    public @Nullable String hostname() { return hostname; }
    public @NotNull ServerStartupEvent hostname(@Nullable String v) { this.hostname = v; return this; }

    public long startedEpochMs() { return startedEpochMs; }
    public @NotNull ServerStartupEvent startedEpochMs(long v) { this.startedEpochMs = v; return this; }

    public long lastHeartbeatEpochMs() { return lastHeartbeatEpochMs; }
    public @NotNull ServerStartupEvent lastHeartbeatEpochMs(long v) { this.lastHeartbeatEpochMs = v; return this; }

    public long closedAtEpochMs() { return closedAtEpochMs; }
    public @NotNull ServerStartupEvent closedAtEpochMs(long v) { this.closedAtEpochMs = v; return this; }

    public @Nullable String closeReason() { return closeReason; }
    public @NotNull ServerStartupEvent closeReason(@Nullable String v) { this.closeReason = v; return this; }

    public @Nullable byte[] verboseManifest() { return verboseManifest; }
    public @NotNull ServerStartupEvent verboseManifest(@Nullable byte[] v) {
        this.verboseManifest = v;
        return this;
    }

    public void reset() {
        startupId = null;
        instanceId = null;
        serverName = null;
        grimVersion = null;
        serverVersionString = null;
        hostname = null;
        startedEpochMs = 0L;
        lastHeartbeatEpochMs = 0L;
        closedAtEpochMs = 0L;
        closeReason = null;
        verboseManifest = null;
    }
}
