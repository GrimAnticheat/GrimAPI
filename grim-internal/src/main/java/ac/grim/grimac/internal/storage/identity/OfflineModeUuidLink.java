package ac.grim.grimac.internal.storage.identity;

import ac.grim.grimac.api.storage.identity.NameResolverLink;
import org.jetbrains.annotations.ApiStatus;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Offline-mode UUID derivation: OfflinePlayer UUIDs are produced by
 * {@code UUID.nameUUIDFromBytes(("OfflinePlayer:" + name).getBytes(UTF-8))}. Used only
 * on offline-mode servers; online-mode deployments keep this link out of the chain.
 * <p>
 * By design this link cannot resolve uuid → name (there's no reverse of nameUUIDFromBytes
 * without a dictionary of candidate names) — it returns empty on {@code resolveByUuid}.
 */
@ApiStatus.Internal
public final class OfflineModeUuidLink implements NameResolverLink {

    public static final String ID = "offline-mode-uuid";

    @Override
    public String id() {
        return ID;
    }

    @Override
    public CompletionStage<Optional<UUID>> resolveByName(String name) {
        UUID derived = UUID.nameUUIDFromBytes(("OfflinePlayer:" + name).getBytes(StandardCharsets.UTF_8));
        return CompletableFuture.completedStage(Optional.of(derived));
    }

    @Override
    public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
        return CompletableFuture.completedStage(Optional.empty());
    }
}
