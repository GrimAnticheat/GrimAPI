package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendConfigSource;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendProvider;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlBackendTimeoutConfigTest {

    @Test
    void mysqlConfigDefaultsMissingAndNonPositiveTimeouts() {
        MysqlBackendConfig missing = (MysqlBackendConfig) new MysqlBackendProvider().readConfig(new MapSource(Map.of()));
        assertEquals(MysqlBackendConfig.DEFAULT_CONNECT_TIMEOUT_MS, missing.connectTimeoutMs());
        assertEquals(MysqlBackendConfig.DEFAULT_SOCKET_TIMEOUT_MS, missing.socketTimeoutMs());

        MysqlBackendConfig nonPositive = new MysqlBackendConfig(
                "localhost", 3306, "grim", "root", null, "", 256,
                0, -1, 1, Map.of(), null, ac.grim.grimac.api.storage.config.TableNames.DEFAULTS);
        assertEquals(MysqlBackendConfig.DEFAULT_CONNECT_TIMEOUT_MS, nonPositive.connectTimeoutMs());
        assertEquals(MysqlBackendConfig.DEFAULT_SOCKET_TIMEOUT_MS, nonPositive.socketTimeoutMs());
    }

    @Test
    void mysqlProviderReadsTimeoutOverrides() {
        MysqlBackendConfig config = (MysqlBackendConfig) new MysqlBackendProvider().readConfig(new MapSource(Map.of(
                "connect-timeout-ms", 7_500,
                "socket-timeout-ms", 90_000)));

        assertEquals(7_500, config.connectTimeoutMs());
        assertEquals(90_000, config.socketTimeoutMs());
    }

    @Test
    void postgresConfigDefaultsMissingAndNonPositiveTimeouts() {
        PostgresBackendConfig missing =
                (PostgresBackendConfig) new PostgresBackendProvider().readConfig(new MapSource(Map.of()));
        assertEquals(PostgresBackendConfig.DEFAULT_CONNECT_TIMEOUT_MS, missing.connectTimeoutMs());
        assertEquals(PostgresBackendConfig.DEFAULT_SOCKET_TIMEOUT_MS, missing.socketTimeoutMs());

        PostgresBackendConfig nonPositive = new PostgresBackendConfig(
                "localhost", 5432, "grim", "postgres", null, "", 256,
                -5, 0, 1, Map.of(), null, ac.grim.grimac.api.storage.config.TableNames.DEFAULTS);
        assertEquals(PostgresBackendConfig.DEFAULT_CONNECT_TIMEOUT_MS, nonPositive.connectTimeoutMs());
        assertEquals(PostgresBackendConfig.DEFAULT_SOCKET_TIMEOUT_MS, nonPositive.socketTimeoutMs());
    }

    @Test
    void postgresProviderReadsTimeoutOverrides() {
        PostgresBackendConfig config =
                (PostgresBackendConfig) new PostgresBackendProvider().readConfig(new MapSource(Map.of(
                        "connect-timeout-ms", 8_000,
                        "socket-timeout-ms", 120_000)));

        assertEquals(8_000, config.connectTimeoutMs());
        assertEquals(120_000, config.socketTimeoutMs());
    }

    private record MapSource(@NotNull Map<String, ?> values) implements BackendConfigSource {
        @Override
        public @NotNull String getString(@NotNull String key, @NotNull String defaultValue) {
            Object value = values.get(key);
            return value == null ? defaultValue : Objects.toString(value);
        }

        @Override
        public int getInt(@NotNull String key, int defaultValue) {
            Object value = values.get(key);
            return value instanceof Number n ? n.intValue() : defaultValue;
        }

        @Override
        public long getLong(@NotNull String key, long defaultValue) {
            Object value = values.get(key);
            return value instanceof Number n ? n.longValue() : defaultValue;
        }

        @Override
        public boolean getBoolean(@NotNull String key, boolean defaultValue) {
            Object value = values.get(key);
            return value instanceof Boolean b ? b : defaultValue;
        }

        @Override
        public @NotNull List<String> getStringList(@NotNull String key, @NotNull List<String> defaultValue) {
            return defaultValue;
        }
    }
}
