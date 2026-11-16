package ac.grim.grimac.internal.storage.backend.postgres;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.sql.HikariPoolSettings;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public record PostgresBackendConfig(
        @NotNull String host,
        int port,
        @NotNull String database,
        @NotNull String user,
        @Nullable String password,
        @NotNull String extraJdbcParams,
        int batchFlushCap,
        int connectTimeoutMs,
        int socketTimeoutMs,
        int writerThreadsDefault,
        @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
        @NotNull HikariPoolSettings poolSettings,
        @NotNull TableNames tableNames) implements BackendConfig {

    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 60_000;
    public static final int DEFAULT_MAXIMUM_POOL_SIZE = 5;

    public PostgresBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (connectTimeoutMs <= 0) connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        if (socketTimeoutMs <= 0) socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        if (writerThreadsDefault <= 0) writerThreadsDefault = 1;
        if (writerThreadsPerCategory == null) writerThreadsPerCategory = java.util.Map.of();
        if (poolSettings == null) poolSettings = HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE);
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public PostgresBackendConfig(@NotNull String host, int port, @NotNull String database,
                              @NotNull String user, @Nullable String password,
                              @NotNull String extraJdbcParams, int batchFlushCap,
                              int writerThreadsDefault,
                              @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
                              @NotNull HikariPoolSettings poolSettings,
                              @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SOCKET_TIMEOUT_MS,
             writerThreadsDefault, writerThreadsPerCategory, poolSettings, tableNames);
    }

    public PostgresBackendConfig(@NotNull String host, int port, @NotNull String database,
                                 @NotNull String user, @Nullable String password,
                                 @NotNull String extraJdbcParams, int batchFlushCap,
                                 int writerThreadsDefault,
                                 @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
                                 @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SOCKET_TIMEOUT_MS,
             writerThreadsDefault, writerThreadsPerCategory,
             HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE), tableNames);
    }

    public PostgresBackendConfig(@NotNull String host, int port, @NotNull String database,
                                    @NotNull String user, @Nullable String password,
                                    @NotNull String extraJdbcParams, int batchFlushCap,
                                    @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SOCKET_TIMEOUT_MS,
             1, java.util.Map.of(),
             HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE), tableNames);
    }

    public int writerThreadsFor(@NotNull String categoryId) {
        return writerThreadsPerCategory.getOrDefault(categoryId, writerThreadsDefault);
    }

    public String jdbcUrl() {
        StringBuilder sb = new StringBuilder("jdbc:postgresql://").append(host).append(':').append(port)
                .append('/').append(database);
        if (!extraJdbcParams.isEmpty()) sb.append('?').append(extraJdbcParams);
        return sb.toString();
    }
}
