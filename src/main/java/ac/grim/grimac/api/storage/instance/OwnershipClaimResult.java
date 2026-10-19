package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Result of an atomic ownership claim attempt.
 *
 * <p>For a successful claim, {@code previousOwner} is the row replaced by the
 * claim, read atomically with that write. It is null only when the claim created
 * a previously absent row. A separate pre-claim read is not sufficient: callers
 * use the previous startup and fence to detect an intervening owner.</p>
 */
@ApiStatus.Experimental
public record OwnershipClaimResult(
        boolean claimed,
        @NotNull UUID persistentId,
        @NotNull UUID startupId,
        @NotNull UUID fence,
        long dbNowEpochMs,
        long leaseExpiresAtEpochMs,
        @Nullable ServerOwnershipSnapshot previousOwner,
        @Nullable ServerOwnershipSnapshot currentOwner) {

    public static @NotNull OwnershipClaimResult claimed(
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long dbNowEpochMs,
            long leaseExpiresAtEpochMs,
            @Nullable ServerOwnershipSnapshot previousOwner) {
        return new OwnershipClaimResult(
                true, persistentId, startupId, fence, dbNowEpochMs,
                leaseExpiresAtEpochMs, previousOwner, null);
    }

    public static @NotNull OwnershipClaimResult denied(
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long dbNowEpochMs,
            @NotNull ServerOwnershipSnapshot currentOwner) {
        return new OwnershipClaimResult(
                false, persistentId, startupId, fence, dbNowEpochMs,
                currentOwner.leaseExpiresAtEpochMs(), null, currentOwner);
    }
}
