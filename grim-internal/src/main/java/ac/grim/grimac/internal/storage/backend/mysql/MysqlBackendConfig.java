package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.sql.HikariPoolSettings;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

@ApiStatus.Internal
public record MysqlBackendConfig(
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
    public static final int DEFAULT_MAXIMUM_POOL_SIZE = 10;

    public MysqlBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (connectTimeoutMs <= 0) connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        if (socketTimeoutMs <= 0) socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
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
                              @NotNull HikariPoolSettings poolSettings,
                              @NotNull TableNames tableNames) {
        this(host, port, database, user, password, extraJdbcParams, batchFlushCap,
             DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_SOCKET_TIMEOUT_MS,
             writerThreadsDefault, writerThreadsPerCategory, poolSettings, tableNames);
    }

    public MysqlBackendConfig(@NotNull String host, int port, @NotNull String database,
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

    public MysqlBackendConfig(@NotNull String host, int port, @NotNull String database,
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

    /**
     * The JDBC URL. TLS is off and public-key retrieval on by default; an operator who sets
     * any TLS parameter ({@code sslMode}, {@code useSSL}, {@code requireSSL}) or
     * {@code allowPublicKeyRetrieval} in the extra parameters gets exactly their setting,
     * because the defaults for those keys are then left out rather than sent twice.
     */
    public String jdbcUrl() {
        Set<String> keys = new HashSet<>();
        for (String param : extraJdbcParams.split("&")) {
            int eq = param.indexOf('=');
            String key = (eq < 0 ? param : param.substring(0, eq)).trim();
            if (!key.isEmpty()) keys.add(key.toLowerCase(Locale.ROOT));
        }
        StringBuilder sb = new StringBuilder("jdbc:mysql://").append(host).append(':').append(port)
                .append('/').append(database).append('?');
        if (!keys.contains("sslmode") && !keys.contains("usessl") && !keys.contains("requiressl")) {
            sb.append("useSSL=false&");
        }
        if (!keys.contains("allowpublickeyretrieval")) sb.append("allowPublicKeyRetrieval=true&");
        sb.append("rewriteBatchedStatements=true");
        if (!extraJdbcParams.isEmpty()) sb.append('&').append(extraJdbcParams);
        return sb.toString();
    }
}
