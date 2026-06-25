package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Result of renewing the current ownership lease.
 */
@ApiStatus.Experimental
public record OwnershipRenewResult(
        boolean renewed,
        @NotNull UUID persistentId,
        @NotNull UUID startupId,
        @NotNull UUID fence,
        long dbNowEpochMs,
        long leaseExpiresAtEpochMs) {

    public static @NotNull OwnershipRenewResult renewed(
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long dbNowEpochMs,
            long leaseExpiresAtEpochMs) {
        return new OwnershipRenewResult(
                true, persistentId, startupId, fence, dbNowEpochMs, leaseExpiresAtEpochMs);
    }

    public static @NotNull OwnershipRenewResult lost(
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long dbNowEpochMs) {
        return new OwnershipRenewResult(
                false, persistentId, startupId, fence, dbNowEpochMs, dbNowEpochMs);
    }
}
