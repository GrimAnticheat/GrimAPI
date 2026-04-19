package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.api.storage.backend.BackendRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ApiStatus.Internal
public final class BackendRegistryImpl implements BackendRegistry {

    private final ConcurrentHashMap<String, BackendProvider> providers = new ConcurrentHashMap<>();

    @Override
    public void register(@NotNull BackendProvider provider) {
        Objects.requireNonNull(provider, "provider");
        String id = Objects.requireNonNull(provider.id(), "provider.id()");
        if (id.isBlank()) {
            throw new IllegalArgumentException("provider id must not be blank");
        }
        providers.put(id, provider);
    }

    @Override
    public @Nullable BackendProvider lookup(@NotNull String id) {
        Objects.requireNonNull(id, "id");
        return providers.get(id);
    }

    @Override
    public @NotNull Set<@NotNull String> registeredIds() {
        return Set.copyOf(providers.keySet());
    }
}
