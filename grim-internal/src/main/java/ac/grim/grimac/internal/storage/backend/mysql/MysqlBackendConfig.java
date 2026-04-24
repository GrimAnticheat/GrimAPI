package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
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
        @NotNull TableNames tableNames) implements BackendConfig {

    public MysqlBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public String jdbcUrl() {
        StringBuilder sb = new StringBuilder("jdbc:mysql://").append(host).append(':').append(port)
                .append('/').append(database)
                .append("?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true");
        if (!extraJdbcParams.isEmpty()) sb.append('&').append(extraJdbcParams);
        return sb.toString();
    }
}
