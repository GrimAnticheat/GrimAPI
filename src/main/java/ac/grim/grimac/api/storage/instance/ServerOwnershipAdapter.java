package ac.grim.grimac.api.storage.instance;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.UUID;

/**
 * Backend-specific primitive for authoritative DB-writer ownership.
 *
 * <p>This is intentionally not modeled as a normal Entity upsert: claim and
 * renew require compare-and-set semantics and affected-row style results.</p>
 */
@ApiStatus.Experimental
public interface ServerOwnershipAdapter {

    void ensureStore(@NotNull StoreId id) throws BackendException;

    long dbNowEpochMs() throws BackendException;

    @NotNull OwnershipClaimResult claimOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs,
            @NotNull ServerOwnershipMetadata metadata) throws BackendException;

    @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException;

    boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException;

    @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException;
}
