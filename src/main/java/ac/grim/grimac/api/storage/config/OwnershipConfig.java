package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Persistent server UUID ownership and crash-recovery timing settings.
 */
@ApiStatus.Experimental
public record OwnershipConfig(
        boolean enforcePersistentUuidOwnership,
        @NotNull DuplicatePersistentUuidAction duplicatePersistentUuidAction,
        long leaseTtlMs,
        long renewIntervalMs,
        long startupWaitMs,
        long safetyMarginMs,
        long staleStartupTtlMs,
        long recoverySweepIntervalMs,
        boolean cleanupOtherServers) {

    public OwnershipConfig {
        duplicatePersistentUuidAction = duplicatePersistentUuidAction == null
                ? DuplicatePersistentUuidAction.DISABLE_STORAGE
                : duplicatePersistentUuidAction;
        leaseTtlMs = Math.max(1L, leaseTtlMs);
        renewIntervalMs = Math.max(1L, renewIntervalMs);
        startupWaitMs = Math.max(0L, startupWaitMs);
        safetyMarginMs = Math.max(0L, safetyMarginMs);
        staleStartupTtlMs = Math.max(1L, staleStartupTtlMs);
        recoverySweepIntervalMs = Math.max(1L, recoverySweepIntervalMs);
    }

    public static @NotNull OwnershipConfig defaults() {
        return new OwnershipConfig(
                true,
                DuplicatePersistentUuidAction.DISABLE_STORAGE,
                20_000L,
                10_000L,
                20_000L,
                5_000L,
                30_000L,
                10_000L,
                true);
    }
}
