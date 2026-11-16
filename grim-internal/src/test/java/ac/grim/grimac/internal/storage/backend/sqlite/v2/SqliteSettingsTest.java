package ac.grim.grimac.internal.storage.backend.sqlite.v2;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** databases/sqlite.yml's journal-mode, synchronous-mode and cache-pages reach the connection. */
@DisplayName("SQLite backend applies its configured settings")
class SqliteSettingsTest {

    @Test
    void configuredSettingsApply(@TempDir Path dir) throws Exception {
        assertSettings(dir, new SqliteBackendConfig("a.db", "delete", "FULL", 5000, 1234, 256, TableNames.DEFAULTS),
            "delete", 2, 1234);
    }

    @Test
    void defaultsAndUnknownValues(@TempDir Path dir) throws Exception {
        assertSettings(dir, SqliteBackendConfig.defaults("b.db"), "wal", 1, 10_000);
        assertSettings(dir, new SqliteBackendConfig("c.db", "WAL; DROP TABLE x", "sometimes", 5000, 0, 256,
            TableNames.DEFAULTS), "wal", 1, -2000);
    }

    private static void assertSettings(Path dir, SqliteBackendConfig cfg, String journal, int synchronous,
                                       int cachePages) throws Exception {
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        backend.init(new BackendContext() {
            @Override public Logger logger() { return Logger.getAnonymousLogger(); }
            @Override public Path dataDirectory() { return dir; }
            @Override public BackendConfig config() { return cfg; }
        });
        try (Connection c = backend.unwrap(DataSource.class).orElseThrow().getConnection();
             Statement s = c.createStatement()) {
            assertEquals(journal, value(s, "PRAGMA journal_mode"));
            assertEquals(String.valueOf(synchronous), value(s, "PRAGMA synchronous"));
            assertEquals(String.valueOf(cachePages), value(s, "PRAGMA cache_size"));
        } finally {
            backend.close();
        }
    }

    private static String value(Statement s, String sql) throws Exception {
        try (ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }
}
