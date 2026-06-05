package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.SessionBlobRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Mutable write-path slot for the {@code SESSION} category. Session writes are
 * idempotent upserts (keyed on {@code sessionId}), so producers typically reuse
 * the event for every heartbeat on a session. Immutable read counterpart is
 * {@link SessionRecord}.
 * <p>
 * {@code clientVersion} is a PacketEvents protocol-version number (PVN). Producers
 * pass {@code user.getClientVersion().getProtocolVersion()}; use {@code -1} for
 * unresolved / unknown. The API side is a plain int so this package carries
 * no PacketEvents dependency; renderers resolve back to display names via PE
 * at the point where chat components are built.
 * <p>
 * {@code sessionBlobs} is an internally mutable list held on the event. It is
 * cleared on {@link #reset()}; producers append via {@link #addSessionBlob(SessionBlobRecord)}
 * or {@link #replaceSessionBlobs(List)}.
 */
@ApiStatus.Experimental
public final class SessionEvent {

    private UUID sessionId;
    private UUID playerUuid;
    private @Nullable String serverName;
    private long startedEpochMs;
    private long lastActivityEpochMs;
    private long closedAtEpochMs;
    private @Nullable String grimVersion;
    private @Nullable String clientBrand;
    private int clientVersion = -1;
    private @Nullable String serverVersionString;
    private @Nullable UUID instanceId;
    private @Nullable UUID startupId;
    private final List<SessionBlobRecord> sessionBlobs = new ArrayList<>();

    public @NotNull UUID sessionId() { return sessionId; }
    public @NotNull SessionEvent sessionId(@NotNull UUID v) { this.sessionId = v; return this; }

    public @NotNull UUID playerUuid() { return playerUuid; }
    public @NotNull SessionEvent playerUuid(@NotNull UUID v) { this.playerUuid = v; return this; }

    public @Nullable String serverName() { return serverName; }
    public @NotNull SessionEvent serverName(@Nullable String v) { this.serverName = v; return this; }

    public long startedEpochMs() { return startedEpochMs; }
    public @NotNull SessionEvent startedEpochMs(long v) { this.startedEpochMs = v; return this; }

    public long lastActivityEpochMs() { return lastActivityEpochMs; }
    public @NotNull SessionEvent lastActivityEpochMs(long v) { this.lastActivityEpochMs = v; return this; }

    /**
     * When the session ended, in epoch milliseconds. {@code 0L} (mirrors
     * {@link SessionRecord#OPEN}) while the session is still alive. The live
     * writer leaves it 0 on every heartbeat; the disconnect/close path sets
     * it to {@code System.currentTimeMillis()} on the final upsert; the
     * startup crash sweep sets it from {@code last_activity} for sessions
     * whose connections went away without a graceful close.
     */
    public long closedAtEpochMs() { return closedAtEpochMs; }
    public @NotNull SessionEvent closedAtEpochMs(long v) { this.closedAtEpochMs = v; return this; }
    /** Mirrors {@link SessionRecord#isClosed()} — {@code closedAtEpochMs != 0L}. */
    public boolean isClosed() { return closedAtEpochMs != 0L; }

    public @Nullable String grimVersion() { return grimVersion; }
    public @NotNull SessionEvent grimVersion(@Nullable String v) { this.grimVersion = v; return this; }

    public @Nullable String clientBrand() { return clientBrand; }
    public @NotNull SessionEvent clientBrand(@Nullable String v) { this.clientBrand = v; return this; }

    public int clientVersion() { return clientVersion; }
    public @NotNull SessionEvent clientVersion(int v) { this.clientVersion = v; return this; }

    public @Nullable String serverVersionString() { return serverVersionString; }
    public @NotNull SessionEvent serverVersionString(@Nullable String v) { this.serverVersionString = v; return this; }

    /**
     * Owning JVM instance id, stamped from the active
     * {@code ServerInstanceRecord.instanceId} for multi-server crash
     * sweep purposes. {@code null} means "no owning instance"
     * (legacy data path or single-server bootstrap before the
     * registry is initialized). Live writers always set this.
     */
    public @Nullable UUID instanceId() { return instanceId; }
    public @NotNull SessionEvent instanceId(@Nullable UUID v) { this.instanceId = v; return this; }

    /**
     * Durable startup row owning this session. New V2 writers stamp this
     * instead of persisting server metadata on every session row.
     */
    public @Nullable UUID startupId() { return startupId; }
    public @NotNull SessionEvent startupId(@Nullable UUID v) { this.startupId = v; return this; }

    public @NotNull List<SessionBlobRecord> sessionBlobs() { return sessionBlobs; }

    public @NotNull SessionEvent addSessionBlob(@NotNull SessionBlobRecord blob) {
        sessionBlobs.add(blob);
        return this;
    }

    public @NotNull SessionEvent replaceSessionBlobs(@Nullable List<SessionBlobRecord> blobs) {
        sessionBlobs.clear();
        if (blobs != null) sessionBlobs.addAll(blobs);
        return this;
    }

    public void reset() {
        sessionId = null;
        playerUuid = null;
        serverName = null;
        startedEpochMs = 0L;
        lastActivityEpochMs = 0L;
        closedAtEpochMs = 0L;
        grimVersion = null;
        clientBrand = null;
        clientVersion = -1;
        serverVersionString = null;
        instanceId = null;
        startupId = null;
        sessionBlobs.clear();
    }
}
