package ac.grim.grimac.api.storage.identity;

import org.jetbrains.annotations.ApiStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

@ApiStatus.Experimental
public interface NameResolver {

    CompletionStage<Optional<UUID>> resolveByName(String name);

    CompletionStage<Optional<String>> resolveByUuid(UUID uuid);

    CompletionStage<List<UUID>> allHistoricalUsersOfName(String name);
}
