package ac.grim.grimac.api.storage.model;

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
 */
@ApiStatus.Experimental
public record SessionRecord(
        UUID sessionId,
        UUID playerUuid,
        String serverName,
        long startedEpochMs,
        long lastActivityEpochMs,
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
