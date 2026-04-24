package ac.grim.grimac.api.storage.identity;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

@ApiStatus.Experimental
public interface NameResolver {

    @NotNull CompletionStage<Optional<UUID>> resolveByName(@NotNull String name);

    @NotNull CompletionStage<Optional<String>> resolveByUuid(@NotNull UUID uuid);

    @NotNull CompletionStage<List<UUID>> allHistoricalUsersOfName(@NotNull String name);
}
