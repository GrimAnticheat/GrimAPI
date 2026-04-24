package ac.grim.grimac.internal.storage.backend.redis;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public record RedisBackendConfig(
        @NotNull String host,
        int port,
        int database,
        @Nullable String user,
        @Nullable String password,
        @NotNull String keyPrefix,
        int timeoutMs,
        int batchFlushCap,
        boolean warnOnHistory,
        @NotNull TableNames tableNames) implements BackendConfig {

    public RedisBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (timeoutMs <= 0) timeoutMs = 2000;
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
        if (keyPrefix == null) keyPrefix = "";
    }
}
