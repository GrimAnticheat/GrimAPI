package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.sql.HikariPoolSettings;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Internal
public record MysqlBackendConfig(
        @NotNull String host,
        int port,
        @NotNull String database,
        @NotNull String user,
        @Nullable String password,
        @NotNull String extraJdbcParams,
        int batchFlushCap,
        int writerThreadsDefault,
        @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
        @NotNull HikariPoolSettings poolSettings,
        @NotNull TableNames tableNames) implements BackendConfig {

    public static final int DEFAULT_MAXIMUM_POOL_SIZE = 10;

    public MysqlBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (writerThreadsDefault <= 0) writerThreadsDefault = 1;
        if (writerThreadsPerCategory == null) writerThreadsPerCategory = java.util.Map.of();
        if (poolSettings == null) poolSettings = HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE);
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public MysqlBackendConfig(@NotNull String host, int port, @NotNull String database,
                              @NotNull String user, @Nullable String password,
                              @NotNull String extraJdbcParams, int batchFlushCap,
                              int writerThreadsDefault,
                              @NotNull java.util.Map<String, Integer> writerThreadsPerCategory,
                              @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             writerThreadsDefault, writerThreadsPerCategory,
             HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE), tableNames);
    }

    public MysqlBackendConfig(@NotNull String host, int port, @NotNull String database,
                              @NotNull String user, @Nullable String password,
                              @NotNull String extraJdbcParams, int batchFlushCap,
                              @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             1, java.util.Map.of(), HikariPoolSettings.defaults(DEFAULT_MAXIMUM_POOL_SIZE), tableNames);
    }

    public int writerThreadsFor(@NotNull String categoryId) {
        return writerThreadsPerCategory.getOrDefault(categoryId, writerThreadsDefault);
    }

    public String jdbcUrl() {
        StringBuilder sb = new StringBuilder("jdbc:mysql://").append(host).append(':').append(port)
                .append('/').append(database)
                .append("?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true");
        if (!extraJdbcParams.isEmpty()) sb.append('&').append(extraJdbcParams);
        return sb.toString();
    }
}
