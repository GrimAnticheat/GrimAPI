package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Internal
public record SqliteBackendConfig(
        String path,
        String journalMode,
        String synchronousMode,
        int busyTimeoutMs,
        int cachePages) implements BackendConfig {

    public static SqliteBackendConfig defaults(String path) {
        return new SqliteBackendConfig(path, "WAL", "NORMAL", 5000, 10_000);
    }
}
