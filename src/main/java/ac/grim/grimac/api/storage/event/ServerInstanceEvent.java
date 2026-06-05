package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.ServerInstanceRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Mutable write-path slot for the server instance registry. Producers
 * upsert this on heartbeat (one per instance per ~30s) — small, cheap,
 * indexed by {@code instanceId}. Immutable read counterpart is
 * {@link ServerInstanceRecord}.
 * <p>
 * The same event slot covers register (first heartbeat after startup),
 * heartbeat (recurring), and drain (final heartbeat with
 * {@code drainMode=true} before graceful shutdown). The registry
 * adapter dedupes via upsert on {@code instanceId}.
 */
@ApiStatus.Experimental
public final class ServerInstanceEvent {

    private @Nullable UUID instanceId;
    private @Nullable String serverName;
    private long startedEpochMs;
    private long lastHeartbeatEpochMs;
    private @Nullable String hostname;
    private @Nullable String grimVersion;
    private boolean drainMode;

    public @Nullable UUID instanceId() { return instanceId; }
    public @NotNull ServerInstanceEvent instanceId(@NotNull UUID v) { this.instanceId = v; return this; }

    public @Nullable String serverName() { return serverName; }
    public @NotNull ServerInstanceEvent serverName(@NotNull String v) { this.serverName = v; return this; }

    public long startedEpochMs() { return startedEpochMs; }
    public @NotNull ServerInstanceEvent startedEpochMs(long v) { this.startedEpochMs = v; return this; }

    public long lastHeartbeatEpochMs() { return lastHeartbeatEpochMs; }
    public @NotNull ServerInstanceEvent lastHeartbeatEpochMs(long v) { this.lastHeartbeatEpochMs = v; return this; }

    public @Nullable String hostname() { return hostname; }
    public @NotNull ServerInstanceEvent hostname(@Nullable String v) { this.hostname = v; return this; }

    public @Nullable String grimVersion() { return grimVersion; }
    public @NotNull ServerInstanceEvent grimVersion(@Nullable String v) { this.grimVersion = v; return this; }

    public boolean drainMode() { return drainMode; }
    public @NotNull ServerInstanceEvent drainMode(boolean v) { this.drainMode = v; return this; }

    public void reset() {
        instanceId = null;
        serverName = null;
        startedEpochMs = 0L;
        lastHeartbeatEpochMs = 0L;
        hostname = null;
        grimVersion = null;
        drainMode = false;
    }
}
