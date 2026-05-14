package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Immutable read-side violation row. The {@code id} is a UUIDv7 generated on
 * the write path; its timestamp prefix gives storage engines a sortable,
 * globally unique tie-breaker while {@code occurredEpochMs} remains the event
 * time used for history ordering.
 */
@ApiStatus.Experimental
public record ViolationRecord(
        UUID id,
        UUID sessionId,
        UUID playerUuid,
        int checkId,
        double vl,
        long occurredEpochMs,
        @Nullable String verbose,
        VerboseFormat verboseFormat) {

    public ViolationRecord {
        if (id == null) throw new IllegalArgumentException("id");
        if (sessionId == null) throw new IllegalArgumentException("sessionId");
        if (playerUuid == null) throw new IllegalArgumentException("playerUuid");
        if (verboseFormat == null) throw new IllegalArgumentException("verboseFormat");
    }
}
