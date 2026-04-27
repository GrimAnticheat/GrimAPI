package ac.grim.grimac.api.storage.identity;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * One link in the {@link NameResolver} chain. Each link is consulted in order; the first
 * link that returns a non-empty result wins. Empty means "don't know, try the next link".
 */
@ApiStatus.Experimental
public interface NameResolverLink {

    @NotNull String id();

    @NotNull CompletionStage<Optional<UUID>> resolveByName(@NotNull String name);

    @NotNull CompletionStage<Optional<String>> resolveByUuid(@NotNull UUID uuid);
}
