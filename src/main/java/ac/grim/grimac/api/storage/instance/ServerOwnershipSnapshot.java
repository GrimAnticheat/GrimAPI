package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Snapshot of the authoritative DB-writer ownership row for one persistent
 * server UUID.
 */
@ApiStatus.Experimental
public record ServerOwnershipSnapshot(
        @NotNull UUID persistentId,
        @NotNull UUID ownerStartupId,
        @NotNull UUID fence,
        long leaseExpiresAtEpochMs,
        long lastRenewedAtEpochMs,
        long closedAtEpochMs,
        @Nullable String closeReason,
        @Nullable String serverName,
        @Nullable String hostname,
        @Nullable String grimVersion,
        @Nullable String serverVersionString) {

    public static final long OPEN = 0L;

    public boolean activeAt(long dbNowEpochMs) {
        return closedAtEpochMs == OPEN && leaseExpiresAtEpochMs > dbNowEpochMs;
    }
}
