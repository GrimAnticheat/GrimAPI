package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcCheckCatalogPersistenceTest {

    @Test
    void insertAllocatesCheckIdsForV2SqlEntityShape(@TempDir Path tempDir) throws Exception {
        String url = "jdbc:sqlite:" + tempDir.resolve("checks.db").toAbsolutePath();
        try (var c = DriverManager.getConnection(url); Statement s = c.createStatement()) {
            s.executeUpdate("""
                CREATE TABLE grim_checks (
                    stable_key TEXT PRIMARY KEY,
                    check_id INTEGER NOT NULL,
                    display TEXT,
                    description TEXT,
                    introduced_version TEXT,
                    introduced_at INTEGER NOT NULL
                )
                """);
            s.executeUpdate("CREATE UNIQUE INDEX grim_checks_by_check_id ON grim_checks (check_id)");
        }

        JdbcCheckCatalogPersistence checks = new JdbcCheckCatalogPersistence(
            () -> DriverManager.getConnection(url), "grim_checks");

        assertEquals(1, checks.insert("movement.simulation", "Simulation", "desc", "2.3.75", 10L));
        assertEquals(2, checks.insert("combat.reach", "Reach", null, "2.3.75", 20L));
        assertEquals(1, checks.insert("movement.simulation", "Simulation", "desc", "2.3.75", 10L));

        List<CheckCatalogRow> rows = new ArrayList<>();
        checks.loadAll().forEach(rows::add);
        Map<Integer, String> stableKeysById = rows.stream()
            .collect(Collectors.toMap(CheckCatalogRow::checkId, CheckCatalogRow::stableKey));
        assertEquals(2, rows.size());
        assertEquals("movement.simulation", stableKeysById.get(1));
        assertEquals("combat.reach", stableKeysById.get(2));
    }
}
