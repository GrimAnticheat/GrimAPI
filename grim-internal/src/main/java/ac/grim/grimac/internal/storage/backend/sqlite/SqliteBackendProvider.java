package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class SqliteBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return SqliteBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return SqliteBackendConfig.class;
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        if (!(config instanceof SqliteBackendConfig c)) {
            throw new IllegalArgumentException(
                    "SqliteBackendProvider requires SqliteBackendConfig, got "
                            + (config == null ? "null" : config.getClass().getName()));
        }
        return new SqliteBackend(c);
    }
}
