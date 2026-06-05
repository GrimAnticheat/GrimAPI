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
        int writerThreadsDefault,
        @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
        @NotNull TableNames tableNames) implements BackendConfig {

    public MongoBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (writerThreadsDefault <= 0) writerThreadsDefault = 1;
        if (writerThreadsPerCategory == null) writerThreadsPerCategory = java.util.Map.of();
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public MongoBackendConfig(@NotNull String connectionString, @NotNull String database,
                              int batchFlushCap, @NotNull TableNames tableNames) {
        this(connectionString, database, batchFlushCap, 1, java.util.Map.of(), tableNames);
    }

    public int writerThreadsFor(@NotNull String categoryId) {
        return writerThreadsPerCategory.getOrDefault(categoryId, writerThreadsDefault);
    }
}
