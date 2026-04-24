package ac.grim.grimac.internal.storage.identity;

import ac.grim.grimac.api.storage.identity.NameResolver;
import ac.grim.grimac.api.storage.identity.NameResolverLink;
import org.jetbrains.annotations.ApiStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Ordered list of {@link NameResolverLink}s. Consulted sequentially; the first
 * non-empty result wins. Empty means "don't know, try the next link".
 */
@ApiStatus.Internal
public final class NameResolverChain implements NameResolver {

    private final List<NameResolverLink> links;

    public NameResolverChain(List<NameResolverLink> links) {
        this.links = List.copyOf(links);
    }

    @Override
    public CompletionStage<Optional<UUID>> resolveByName(String name) {
        return chain(0, link -> link.resolveByName(name));
    }

    @Override
    public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
        return chain(0, link -> link.resolveByUuid(uuid));
    }

    @Override
    public CompletionStage<List<UUID>> allHistoricalUsersOfName(String name) {
        // The chain's links don't yet expose a "list every uuid that used this
        // name" capability — the surface only returns "the currently-resolving
        // uuid". Return the first hit as a singleton so the caller gets
        // consistent behaviour; extending this to a true history walk is a
        // follow-up that needs a new link contract.
        return resolveByName(name).thenApply(opt ->
                opt.<List<UUID>>map(Collections::singletonList).orElseGet(Collections::emptyList));
    }

    private <T> CompletionStage<Optional<T>> chain(int idx, LinkAccess<T> access) {
        if (idx >= links.size()) return CompletableFuture.completedStage(Optional.empty());
        return access.apply(links.get(idx))
                .thenCompose(result -> result.isPresent()
                        ? CompletableFuture.completedStage(result)
                        : chain(idx + 1, access));
    }

    public List<NameResolverLink> links() {
        return new ArrayList<>(links);
    }

    @FunctionalInterface
    private interface LinkAccess<T> {
        CompletionStage<Optional<T>> apply(NameResolverLink link);
    }
}
