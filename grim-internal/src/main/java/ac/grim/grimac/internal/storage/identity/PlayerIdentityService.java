package ac.grim.grimac.internal.storage.identity;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import org.jetbrains.annotations.ApiStatus;

import java.util.UUID;

/**
 * Thin facade for submitting player-identity upserts. Platform glue calls
 * {@link #observe(UUID, String, long)} on join/quit events; backend upsert semantics
 * (merge firstSeen=min, lastSeen=max) live in the backend impls.
 */
@ApiStatus.Internal
public final class PlayerIdentityService {

    private final DataStore store;

    public PlayerIdentityService(DataStore store) {
        this.store = store;
    }

    public void observe(UUID uuid, String name, long epochMs) {
        store.submit(Categories.PLAYER_IDENTITY, new PlayerIdentity(uuid, name, epochMs, epochMs));
    }
}
