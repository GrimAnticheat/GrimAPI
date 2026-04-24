package ac.grim.grimac.internal.storage.backend.mongo;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public final class MongoBackendProvider implements BackendProvider {

    @Override
    public @NotNull String id() {
        return MongoBackend.ID;
    }

    @Override
    public @NotNull Class<? extends BackendConfig> configType() {
        return MongoBackendConfig.class;
    }

    @Override
    public @NotNull BackendConfig readConfig(@NotNull BackendConfigSource src) {
        return new MongoBackendConfig(
                src.getString("connection-string", "mongodb://localhost:27017"),
                src.getString("database", "grim"),
                src.getInt("batch-flush-cap", 256),
                TableNames.readFrom(src));
    }

    @Override
    public @NotNull Backend create(@NotNull BackendConfig config) {
        if (!(config instanceof MongoBackendConfig c)) {
            throw new IllegalArgumentException(
                    "MongoBackendProvider requires MongoBackendConfig, got "
                            + (config == null ? "null" : config.getClass().getName()));
        }
        return new MongoBackend(c);
    }
}
