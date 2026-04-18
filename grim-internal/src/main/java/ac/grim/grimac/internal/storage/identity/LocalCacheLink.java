package ac.grim.grimac.internal.storage.identity;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.identity.NameResolverLink;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.query.Queries;
import org.jetbrains.annotations.ApiStatus;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Name-resolver link backed by the local PlayerIdentity category. Matches the
 * PR #2371-style behaviour: only resolves UUIDs + names we've previously observed on
 * this server (or any other that writes to the same shared networked backend).
 */
@ApiStatus.Internal
public final class LocalCacheLink implements NameResolverLink {

    public static final String ID = "local-cache";

    private final DataStore store;

    public LocalCacheLink(DataStore store) {
        this.store = store;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public CompletionStage<Optional<UUID>> resolveByName(String name) {
        return store.query(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentityByName(name))
                .thenApply(page -> page.items().isEmpty()
                        ? Optional.empty()
                        : Optional.of(page.items().get(0).uuid()));
    }

    @Override
    public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
        return store.query(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(uuid))
                .thenApply(page -> {
                    if (page.items().isEmpty()) return Optional.empty();
                    PlayerIdentity id = page.items().get(0);
                    return Optional.ofNullable(id.currentName());
                });
    }
}
