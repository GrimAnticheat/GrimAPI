package ac.grim.grimac.internal.storage.backend.postgres;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.config.TableNames;
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
        @NotNull TableNames tableNames) implements BackendConfig {

    public PostgresBackendConfig {
        if (batchFlushCap <= 0) batchFlushCap = 256;
        if (tableNames == null) tableNames = TableNames.DEFAULTS;
    }

    public String jdbcUrl() {
        StringBuilder sb = new StringBuilder("jdbc:postgresql://").append(host).append(':').append(port)
                .append('/').append(database);
        if (!extraJdbcParams.isEmpty()) sb.append('?').append(extraJdbcParams);
        return sb.toString();
    }
}
