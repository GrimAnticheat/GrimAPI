package ac.grim.grimac.internal.storage.backend.mysql.v2;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MysqlBackendV2UrlTest {

    @Test
    void usesConfiguredTimeouts() {
        var config = new ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig(
                "db", 3306, "grim", "root", null, "", 256,
                7_500, 90_000, 1, java.util.Map.of(), null,
                ac.grim.grimac.api.storage.config.TableNames.DEFAULTS);

        assertEquals(
                "jdbc:mysql://db:3306/grim?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
                        + "&connectTimeout=7500&socketTimeout=90000",
                MysqlBackendV2.jdbcUrlWithTimeouts(config));
    }

    @Test
    void operatorTlsSettingsReplaceTheDefaults() {
        var config = new ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig(
                "db", 3306, "grim", "root", null, "sslMode=VERIFY_IDENTITY&allowPublicKeyRetrieval=false", 256,
                ac.grim.grimac.api.storage.config.TableNames.DEFAULTS);
        assertEquals("jdbc:mysql://db:3306/grim?rewriteBatchedStatements=true"
                + "&sslMode=VERIFY_IDENTITY&allowPublicKeyRetrieval=false", config.jdbcUrl());

        var legacyFlag = new ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig(
                "db", 3306, "grim", "root", null, "useSSL=true", 256,
                ac.grim.grimac.api.storage.config.TableNames.DEFAULTS);
        assertEquals("jdbc:mysql://db:3306/grim?allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
                + "&useSSL=true", legacyFlag.jdbcUrl());
    }

    @Test
    void appendsTimeoutParamsToBareUrl() {
        String url = MysqlBackendV2.withTimeoutParam("jdbc:mysql://db:3306/grim", "connectTimeout", 5_000L);
        url = MysqlBackendV2.withTimeoutParam(url, "socketTimeout", 60_000L);
        assertEquals("jdbc:mysql://db:3306/grim?connectTimeout=5000&socketTimeout=60000", url);
    }

    @Test
    void appendsWithAmpersandWhenUrlHasQuery() {
        String url = MysqlBackendV2.withTimeoutParam("jdbc:mysql://db:3306/grim?useSSL=false", "socketTimeout", 60_000L);
        assertEquals("jdbc:mysql://db:3306/grim?useSSL=false&socketTimeout=60000", url);
    }

    @Test
    void operatorOverrideWins() {
        String original = "jdbc:mysql://db:3306/grim?socketTimeout=120000";
        assertEquals(original, MysqlBackendV2.withTimeoutParam(original, "socketTimeout", 60_000L));
    }
}
