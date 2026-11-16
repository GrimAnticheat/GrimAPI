package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * On SQLite every write runs on the backend's single writer thread, which logged each
 * failed batch with a full stack trace and did not count the drop. A failure there must
 * reach the rate-limited, counted warning like every other backend's.
 */
@DisplayName("SQLite write failures are counted and rate-limited")
class SqliteWriteFailureIntegrationTest {

    @Test
    void failingWritesLogOnce(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "write-failure.db");
             V2StoreFixture f = new V2StoreFixture(db.backend(), "wf", db.logs().logger())) {
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE " + db.dialect().quoteIdentifier(f.violationsTable));
            }
            db.logs().clear();
            UUID session = UUID.randomUUID();
            UUID player = UUID.randomUUID();
            for (int i = 0; i < 2_000; i++) {
                int n = i;
                f.store.submit(Categories.VIOLATION, e -> e.sessionId(session).playerUuid(player).checkId(1).vl(1)
                    .occurredEpochMs(n).verboseFormat(VerboseFormat.TEXT));
                if (i % 100 == 99) f.store.drain(5_000L);
            }
            f.store.drain(5_000L);

            List<LogRecord> warnings = db.logs().at(Level.WARNING);
            assertEquals(1, warnings.size(), db.logs().messages(Level.WARNING).toString());
            assertNotNull(warnings.get(0).getThrown());
            assertTrue(warnings.get(0).getMessage().contains("failed for violation"), warnings.get(0).getMessage());
        }
    }
}
