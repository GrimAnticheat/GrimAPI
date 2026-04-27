package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public record SqliteBackendConfig(
        @NotNull String path,
        @NotNull String journalMode,
        @NotNull String synchronousMode,
        int busyTimeoutMs,
        int cachePages,
        int batchFlushCap,
        @NotNull TableNames tableNames) implements BackendConfig {

    public SqliteBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public static SqliteBackendConfig defaults(String path) {
        return new SqliteBackendConfig(path, "WAL", "NORMAL", 5000, 10_000, 256, TableNames.DEFAULTS);
    }
}
