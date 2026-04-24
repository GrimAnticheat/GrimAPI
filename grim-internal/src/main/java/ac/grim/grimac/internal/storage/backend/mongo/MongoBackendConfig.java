package ac.grim.grimac.internal.storage.backend.mongo;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public record MongoBackendConfig(
        @NotNull String connectionString,
        @NotNull String database,
        int batchFlushCap,
        @NotNull TableNames tableNames) implements BackendConfig {

    public MongoBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }
}
