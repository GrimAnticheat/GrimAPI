package ac.grim.grimac.internal.storage.backend.postgres;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.sql.HikariPoolSettings;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class PostgresBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return PostgresBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return PostgresBackendConfig.class;
    }

    @Override
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        String pw = src.getString("password", "");
        int wtDefault = src.getInt("writer-threads.default", 1);
        java.util.Map<String, Integer> wtPerCat = new java.util.LinkedHashMap<>();
        for (String cat : new String[]{"violation", "session", "player-identity", "setting", "blob"}) {
            int v = src.getInt("writer-threads." + cat, -1);
            if (v > 0) wtPerCat.put(cat, v);
        }
        return new PostgresBackendConfig(
                src.getString("host", "localhost"),
                src.getInt("port", 5432),
                src.getString("database", "grim"),
                src.getString("user", "postgres"),
                pw.isEmpty() ? null : pw,
                src.getString("extra-jdbc-params", ""),
                src.getInt("batch-flush-cap", 256),
                wtDefault, wtPerCat,
                HikariPoolSettings.readFrom(src, PostgresBackendConfig.DEFAULT_MAXIMUM_POOL_SIZE),
                TableNames.readFrom(src));
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        if (!(config instanceof PostgresBackendConfig c)) {
            throw new IllegalArgumentException(
                    "PostgresBackendProvider requires PostgresBackendConfig, got "
                            + (config == null ? "null" : config.getClass().getName()));
        }
        return new PostgresBackend(c);
    }
}
