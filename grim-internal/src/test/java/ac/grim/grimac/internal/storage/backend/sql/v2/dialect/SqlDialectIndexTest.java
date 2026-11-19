package ac.grim.grimac.internal.storage.backend.sql.v2.dialect;

import ac.grim.grimac.api.storage.kind.IndexSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("SQL v2 dialects: index DDL naming")
class SqlDialectIndexTest {

    private static final IndexSpec BY_NAME = IndexSpec.of("by_name", "name");
    private static final IndexSpec COMPOUND = IndexSpec.unique("by_owner_started", "owner", "-started_at");
    private static final IndexSpec PREFIX = new IndexSpec("by_prefix", List.of("name", "-seen"), false, true);

    @Test @DisplayName("MySQL entity indexes keep the spec name and omit IF NOT EXISTS")
    void mysqlEntityNames() {
        MysqlDialect d = new MysqlDialect();
        assertEquals("CREATE INDEX `by_name` ON `players` (`name`)", d.createIndexSql("players", BY_NAME));
        assertEquals("CREATE UNIQUE INDEX `by_owner_started` ON `players` (`owner`, `started_at` DESC)",
            d.createIndexSql("players", COMPOUND));
        assertEquals("CREATE INDEX `by_prefix` ON `players` (`name`, `seen` DESC)",
            d.createIndexSql("players", PREFIX));
    }

    @Test @DisplayName("Postgres entity indexes keep the table-qualified, truncated name")
    void postgresEntityNames() {
        PostgresDialect d = new PostgresDialect();
        assertEquals("CREATE INDEX IF NOT EXISTS \"players_by_name\" ON \"players\" (\"name\")",
            d.createIndexSql("players", BY_NAME));
        assertEquals("CREATE UNIQUE INDEX IF NOT EXISTS \"players_by_owner_started\" ON \"players\""
            + " (\"owner\", \"started_at\" DESC)", d.createIndexSql("players", COMPOUND));
        assertEquals("CREATE INDEX IF NOT EXISTS \"players_by_prefix\" ON \"players\""
            + " (LOWER(\"name\"), \"seen\" DESC)", d.createIndexSql("players", PREFIX));
        String longTable = "t".repeat(70);
        assertEquals("CREATE INDEX IF NOT EXISTS \"" + "t".repeat(63) + "\" ON \"" + longTable + "\" (\"name\")",
            d.createIndexSql(longTable, BY_NAME));
    }

    @Test @DisplayName("SQLite entity indexes keep the table-prefixed name")
    void sqliteEntityNames() {
        SqliteDialect d = new SqliteDialect();
        assertEquals("CREATE INDEX IF NOT EXISTS \"players_by_name\" ON \"players\" (\"name\")",
            d.createIndexSql("players", BY_NAME));
        assertEquals("CREATE UNIQUE INDEX IF NOT EXISTS \"players_by_owner_started\" ON \"players\""
            + " (\"owner\", \"started_at\" DESC)", d.createIndexSql("players", COMPOUND));
    }

    @Test @DisplayName("explicit physical names are used verbatim and quoted once")
    void explicitPhysicalNames() {
        IndexSpec ts = IndexSpec.of("idx_grim_violations_ts", "occurred_at");
        assertEquals("CREATE INDEX `idx_grim_violations_ts` ON `grim_violations` (`occurred_at`)",
            new MysqlDialect().createIndexSql("grim_violations", ts, "idx_grim_violations_ts"));
        assertEquals("CREATE INDEX IF NOT EXISTS \"idx_grim_violations_ts\" ON \"grim_violations\" (\"occurred_at\")",
            new PostgresDialect().createIndexSql("grim_violations", ts, "idx_grim_violations_ts"));
        assertEquals("CREATE INDEX IF NOT EXISTS \"idx_grim_violations_ts\" ON \"grim_violations\" (\"occurred_at\")",
            new SqliteDialect().createIndexSql("grim_violations", ts, "idx_grim_violations_ts"));

        IndexSpec odd = IndexSpec.of("idx_we`ird\"_col", "co`l\"x");
        assertEquals("CREATE INDEX `idx_we``ird\"_col` ON `we``ird\"` (`co``l\"x`)",
            new MysqlDialect().createIndexSql("we`ird\"", odd, "idx_we`ird\"_col"));
        assertEquals("CREATE INDEX IF NOT EXISTS \"idx_we`ird\"\"_col\" ON \"we`ird\"\"\" (\"co`l\"\"x\")",
            new PostgresDialect().createIndexSql("we`ird\"", odd, "idx_we`ird\"_col"));
        assertEquals("CREATE INDEX IF NOT EXISTS \"idx_we`ird\"\"_col\" ON \"we`ird\"\"\" (\"co`l\"\"x\")",
            new SqliteDialect().createIndexSql("we`ird\"", odd, "idx_we`ird\"_col"));
    }

    @Test @DisplayName("only MySQL 1061 / 42000 counts as a duplicate index name")
    void duplicateIndexClassification() {
        MysqlDialect mysql = new MysqlDialect();
        assertTrue(mysql.isDuplicateIndexError(
            new SQLSyntaxErrorException("Duplicate key name 'idx'", "42000", 1061)));
        assertFalse(mysql.isDuplicateIndexError(
            new SQLSyntaxErrorException("You have an error in your SQL syntax", "42000", 1064)));
        assertFalse(mysql.isDuplicateIndexError(
            new SQLIntegrityConstraintViolationException("Duplicate entry", "23000", 1062)));
        assertFalse(mysql.isDuplicateIndexError(new SQLException("other", "HY000", 1061)));
        assertFalse(mysql.isDuplicateIndexError(new SQLException("denied", "42000", 1142)));
        assertFalse(mysql.isDuplicateIndexError(new SQLException("no state")));

        SQLException mysqlDuplicate = new SQLSyntaxErrorException("Duplicate key name 'idx'", "42000", 1061);
        assertFalse(new PostgresDialect().isDuplicateIndexError(mysqlDuplicate));
        assertFalse(new SqliteDialect().isDuplicateIndexError(mysqlDuplicate));
        assertFalse(new PostgresDialect().isDuplicateIndexError(new SQLException("exists", "42P07", 0)));
    }
}
