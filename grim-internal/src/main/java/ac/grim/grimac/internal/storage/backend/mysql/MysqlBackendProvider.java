package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class MysqlBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return MysqlBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return MysqlBackendConfig.class;
    }

    @Override
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        String pw = src.getString("password", "");
        return new MysqlBackendConfig(
                src.getString("host", "localhost"),
                src.getInt("port", 3306),
                src.getString("database", "grim"),
                src.getString("user", "root"),
                pw.isEmpty() ? null : pw,
                src.getString("extra-jdbc-params", ""),
                src.getInt("batch-flush-cap", 256),
                TableNames.readFrom(src));
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        if (!(config instanceof MysqlBackendConfig c)) {
            throw new IllegalArgumentException(
                    "MysqlBackendProvider requires MysqlBackendConfig, got "
                            + (config == null ? "null" : config.getClass().getName()));
        }
        return new MysqlBackend(c);
    }
}
