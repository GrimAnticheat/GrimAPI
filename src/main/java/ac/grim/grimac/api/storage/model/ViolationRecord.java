package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@ApiStatus.Experimental
public record ViolationRecord(
        long id,
        UUID sessionId,
        UUID playerUuid,
        int checkId,
        double vl,
        long occurredEpochMs,
        @Nullable String verbose,
        VerboseFormat verboseFormat) {

    public ViolationRecord {
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        if (verboseFormat == null) throw new IllegalArgumentException("verboseFormat");
    }
}
