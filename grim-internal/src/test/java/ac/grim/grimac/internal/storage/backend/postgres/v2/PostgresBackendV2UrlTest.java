package ac.grim.grimac.internal.storage.backend.postgres.v2;

import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresBackendV2UrlTest {

    @Test
    void usesConfiguredTimeoutsInPgJdbcSeconds() {
        PostgresBackendConfig config = new PostgresBackendConfig(
                "db", 5432, "grim", "postgres", null, "", 256,
                7_500, 90_000, 1, Map.of(), null, TableNames.DEFAULTS);

        assertEquals(
                "jdbc:postgresql://db:5432/grim?connectTimeout=8&socketTimeout=90",
                PostgresBackendV2.jdbcUrlWithTimeouts(config));
    }

    @Test
    void appendsWithAmpersandWhenUrlHasQuery() {
        String url = PostgresBackendV2.withTimeoutParamSeconds(
                "jdbc:postgresql://db:5432/grim?sslmode=require",
                "socketTimeout",
                60);

        assertEquals("jdbc:postgresql://db:5432/grim?sslmode=require&socketTimeout=60", url);
    }

    @Test
    void operatorOverrideWins() {
        String original = "jdbc:postgresql://db:5432/grim?connectTimeout=30&socketTimeout=120";

        assertEquals(original, PostgresBackendV2.jdbcUrlWithTimeouts(new PostgresBackendConfig(
                "db", 5432, "grim", "postgres", null,
                "connectTimeout=30&socketTimeout=120", 256,
                5_000, 60_000, 1, Map.of(), null, TableNames.DEFAULTS)));
    }

    @Test
    void roundsPositiveMillisecondsUpToAtLeastOneSecond() {
        assertEquals(1, PostgresBackendV2.millisecondsToPgJdbcSeconds(1));
        assertEquals(1, PostgresBackendV2.millisecondsToPgJdbcSeconds(1_000));
        assertEquals(2, PostgresBackendV2.millisecondsToPgJdbcSeconds(1_001));
    }
}
