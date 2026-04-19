package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.ReplayClip;
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
 * {@code replayClips} is an internally mutable list held on the event. It is
 * cleared on {@link #reset()}; producers append via {@link #addReplayClip(ReplayClip)}
 * or {@link #replaceReplayClips(List)}. Phase-1 SQLite storage does not persist
 * non-empty clip lists (recorder is phase-3 work).
 */
@ApiStatus.Experimental
public final class SessionEvent {

    private UUID sessionId;
    private UUID playerUuid;
    private @Nullable String serverName;
    private long startedEpochMs;
    private long lastActivityEpochMs;
    private @Nullable String grimVersion;
    private @Nullable String clientBrand;
    private @Nullable String clientVersionString;
    private @Nullable String serverVersionString;
    private final List<ReplayClip> replayClips = new ArrayList<>();

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

    public @Nullable String grimVersion() { return grimVersion; }
    public @NotNull SessionEvent grimVersion(@Nullable String v) { this.grimVersion = v; return this; }

    public @Nullable String clientBrand() { return clientBrand; }
    public @NotNull SessionEvent clientBrand(@Nullable String v) { this.clientBrand = v; return this; }

    public @Nullable String clientVersionString() { return clientVersionString; }
    public @NotNull SessionEvent clientVersionString(@Nullable String v) { this.clientVersionString = v; return this; }

    public @Nullable String serverVersionString() { return serverVersionString; }
    public @NotNull SessionEvent serverVersionString(@Nullable String v) { this.serverVersionString = v; return this; }

    public @NotNull List<ReplayClip> replayClips() { return replayClips; }

    public @NotNull SessionEvent addReplayClip(@NotNull ReplayClip clip) {
        replayClips.add(clip);
        return this;
    }

    public @NotNull SessionEvent replaceReplayClips(@Nullable List<ReplayClip> clips) {
        replayClips.clear();
        if (clips != null) replayClips.addAll(clips);
        return this;
    }

    public void reset() {
        sessionId = null;
        playerUuid = null;
        serverName = null;
        startedEpochMs = 0L;
        lastActivityEpochMs = 0L;
        grimVersion = null;
        clientBrand = null;
        clientVersionString = null;
        serverVersionString = null;
        replayClips.clear();
    }
}
