package ac.grim.grimac.internal.storage.backend.redis;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class RedisBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return RedisBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return RedisBackendConfig.class;
    }

    @Override
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        String user = src.getString("user", "");
        String pw = src.getString("password", "");
        return new RedisBackendConfig(
                src.getString("host", "localhost"),
                src.getInt("port", 6379),
                src.getInt("database", 0),
                user.isEmpty() ? null : user,
                pw.isEmpty() ? null : pw,
                src.getString("key-prefix", ""),
                src.getInt("timeout-ms", 2000),
                src.getInt("batch-flush-cap", 256),
                src.getBoolean("warn-on-history", true),
                TableNames.readFrom(src));
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        if (!(config instanceof RedisBackendConfig c)) {
            throw new IllegalArgumentException(
                    "RedisBackendProvider requires RedisBackendConfig, got "
                            + (config == null ? "null" : config.getClass().getName()));
        }
        return new RedisBackend(c);
    }
}
