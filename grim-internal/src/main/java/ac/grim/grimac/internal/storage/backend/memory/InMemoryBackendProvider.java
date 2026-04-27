package ac.grim.grimac.internal.storage.backend.memory;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class InMemoryBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return InMemoryBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return InMemoryBackend.Config.class;
    }

    @Override
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        return new InMemoryBackend.Config();
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        return new InMemoryBackend();
    }
}
