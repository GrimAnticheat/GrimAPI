package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

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
 * {@code closedAtEpochMs} is null while the session is alive. Set by the
 * disconnect path once on graceful close, or by the startup crash sweep
 * (using {@code last_activity} as best-estimate) for sessions whose
 * connections went away without firing the disconnect path. Renderers
 * use {@code closedAtEpochMs == null} → "ongoing" and the gap between
 * {@code closedAtEpochMs} and {@code lastActivityEpochMs} → graceful
 * vs crashed distinction.
 */
@ApiStatus.Experimental
public record SessionRecord(
        UUID sessionId,
        UUID playerUuid,
        String serverName,
        long startedEpochMs,
        long lastActivityEpochMs,
        @Nullable Long closedAtEpochMs,
        String grimVersion,
        String clientBrand,
        int clientVersion,
        String serverVersionString,
        List<ReplayClip> replayClips) {

    public SessionRecord {
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        replayClips = replayClips == null ? List.of() : List.copyOf(replayClips);
    }
}
