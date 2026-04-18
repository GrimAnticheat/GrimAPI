package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.UUID;

@ApiStatus.Experimental
public record SessionRecord(
        UUID sessionId,
        UUID playerUuid,
        String serverName,
        long startedEpochMs,
        long lastActivityEpochMs,
        String grimVersion,
        String clientBrand,
        String clientVersionString,
        String serverVersionString,
        List<ReplayClip> replayClips) {

    public SessionRecord {
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        replayClips = replayClips == null ? List.of() : List.copyOf(replayClips);
    }
}
