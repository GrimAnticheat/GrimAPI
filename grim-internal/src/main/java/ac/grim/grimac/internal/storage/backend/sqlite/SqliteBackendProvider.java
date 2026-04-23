package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
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
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        return new SqliteBackendConfig(
                src.getString("path", "data/history.v1.db"),
                src.getString("journal-mode", "WAL"),
                src.getString("synchronous-mode", "NORMAL"),
                src.getInt("busy-timeout-ms", 5000),
                src.getInt("cache-pages", 10_000),
                src.getInt("batch-flush-cap", 256));
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
