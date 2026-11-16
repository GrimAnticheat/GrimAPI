package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.SqlEventStreamAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.MysqlDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real MySQL rejects {@code CREATE INDEX IF NOT EXISTS}, and the event-stream adapter
 * used to ignore that error, so grim_violations had no secondary index on MySQL at all.
 * Every SQL database must end up with the default indexes, a restart or a second
 * server must not warn, and an index that really cannot be created must be reported.
 */
@DisplayName("ensureStore creates the default indexes on every SQL database")
class IndexCreationIntegrationTest {

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "index-creation.db")) {
            scenario(db, false);
            lookalikeName(db);
        }
    }

    @Test
    void postgres() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.postgres()) {
            scenario(db, true);
            lookalikeName(db);
        }
    }

    @Test
    void mysql() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.mysql()) {
            scenario(db, true);
            lookalikeName(db);
        }
    }

    @Test
    void mysqlStatementsNeverSayIfNotExists() {
        MysqlDialect dialect = new MysqlDialect();
        assertFalse(dialect.supportsCreateIndexIfNotExists());
        assertEquals("CREATE INDEX `idx_v_session_id` ON `v` (`session_id`)",
            dialect.createPlainIndexSql("v", "idx_v_session_id", List.of("session_id")));
        assertFalse(dialect.createIndexSql("v", IndexSpec.of("by_x", "x")).contains("IF NOT EXISTS"));
        assertTrue(dialect.isDuplicateIndex(new SQLException("Duplicate key name 'x'", "42000", 1061)));
        assertFalse(dialect.isDuplicateIndex(new SQLException("syntax", "42000", 1064)));
    }

    private static void scenario(SqlTestDatabases.Target db, boolean sharedDatabase) throws Exception {
        EventStream<ViolationEvent, ViolationRecord> violations = V2BuiltinKinds.violations();
        Entity<?, ?, ?> sessions = V2BuiltinKinds.sessions();
        String vTable = SqlTestDatabases.uniqueTable("idxc_violations");
        String sTable = SqlTestDatabases.uniqueTable("idxc_sessions");
        String bTable = SqlTestDatabases.uniqueTable("idxc_broken");
        List<String> vDefaults = List.of("idx_" + vTable + "_session_id", "idx_" + vTable + "_player_uuid",
            "idx_" + vTable + "_check_id", "idx_" + vTable + "_ts");
        List<String> sDefaults = List.of("by_player_started", "by_startup_open", "by_started");
        try {
            ensure(db, violations, vTable);
            ensure(db, sessions, sTable);
            assertIndexes(db, vTable, vDefaults, true);
            assertIndexes(db, sTable, sDefaults, false);
            assertEquals(List.of(), db.logs().messages(Level.WARNING), db.label() + ": first start");

            // A restart finds every default in place and creates nothing.
            db.logs().clear();
            ensure(db, violations, vTable);
            ensure(db, sessions, sTable);
            assertEquals(List.of(), db.logs().messages(Level.WARNING), db.label() + ": restart");
            assertEquals(List.of(), db.logs().messages(Level.INFO), db.label() + ": restart creates nothing");

            if (sharedDatabase) {
                // Another server starting against the same database.
                try (SqlTestDatabases.Target other = db.reopen()) {
                    ensure(other, violations, vTable);
                    ensure(other, sessions, sTable);
                    assertEquals(List.of(), other.logs().messages(Level.WARNING), db.label() + ": second server");
                }
            }

            // A default dropped from a table that has rows comes back, and says so.
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                SqlDialect d = db.dialect();
                String uuid = uuidLiteral(d);
                s.executeUpdate("INSERT INTO " + d.quoteIdentifier(vTable) + " (" + d.quoteIdentifier("id") + ", "
                    + d.quoteIdentifier("session_id") + ", " + d.quoteIdentifier("player_uuid") + ", "
                    + d.quoteIdentifier("check_id") + ", " + d.quoteIdentifier("vl") + ", "
                    + d.quoteIdentifier("occurred_at") + ", " + d.quoteIdentifier("verbose_format")
                    + ") VALUES (" + uuid + ", " + uuid + ", " + uuid + ", 1, 1, 1, 0)");
                String drop = "DROP INDEX " + db.dialect().quoteIdentifier(vDefaults.get(0));
                if (db.dialect().name().equals("mysql")) drop += " ON " + db.dialect().quoteIdentifier(vTable);
                s.executeUpdate(drop);
            }
            db.logs().clear();
            ensure(db, violations, vTable);
            assertIndexes(db, vTable, vDefaults, true);
            assertTrue(db.logs().messages(Level.INFO).stream().anyMatch(m -> m.contains(vDefaults.get(0))),
                db.logs().messages(Level.INFO).toString());

            // An index that cannot be created is reported, and the store still starts.
            DataSource ds = db.backend().unwrap(DataSource.class).orElseThrow();
            SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
            new SqlEventStreamAdapter(ds, new MissingColumnIndexes(db.dialect()), logs.logger())
                .ensureStore(StoreId.grim(bTable), violations);
            List<String> warnings = logs.messages(Level.WARNING);
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).contains("idx_" + bTable + "_ts"), warnings.get(0));
        } finally {
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE IF EXISTS " + db.dialect().quoteIdentifier(vTable));
                s.executeUpdate("DROP TABLE IF EXISTS " + db.dialect().quoteIdentifier(sTable));
                s.executeUpdate("DROP TABLE IF EXISTS " + db.dialect().quoteIdentifier(bTable));
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    /** An unrelated index whose name only ends like the case-insensitive name index does not stand in for it. */
    private static void lookalikeName(SqlTestDatabases.Target db) throws Exception {
        Entity<?, ?, ?> players = V2BuiltinKinds.players();
        String table = SqlTestDatabases.uniqueTable("idxc_players");
        SqlDialect d = db.dialect();
        String byName = d.physicalIndexName(table, "by_name");
        try {
            ensure(db, players, table);
            assertIndexes(db, table, List.of(byName), true);
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                String drop = "DROP INDEX " + d.quoteIdentifier(byName);
                if (d.name().equals("mysql")) drop += " ON " + d.quoteIdentifier(table);
                s.executeUpdate(drop);
                s.executeUpdate(d.createPlainIndexSql(table, table + "_legacy_by_name", List.of("first_seen")));
            }
            ensure(db, players, table);
            assertIndexes(db, table, List.of(byName), true);
        } finally {
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE IF EXISTS " + d.quoteIdentifier(table));
            }
        }
    }

    private static void ensure(SqlTestDatabases.Target db, Object kind, String table) throws Exception {
        KindAdapter adapter = (KindAdapter) db.backend().adapterFor((DataKind) kind).orElseThrow();
        adapter.ensureStore(StoreId.grim(table), (DataKind) kind);
    }

    private static void assertIndexes(SqlTestDatabases.Target db, String table, List<String> defaults, boolean exact)
            throws SQLException {
        List<String> names = new ArrayList<>();
        try (Connection c = db.open()) {
            for (IndexCoverage.Existing index : db.dialect().existingIndexes(c, table)) {
                names.add(index.name().toLowerCase(Locale.ROOT));
            }
        }
        for (String index : defaults) {
            String lower = index.toLowerCase(Locale.ROOT);
            boolean found = exact ? names.contains(lower) : names.stream().anyMatch(n -> n.endsWith(lower));
            assertTrue(found, db.label() + ": " + index + " missing on " + table + ": " + names);
        }
    }

    private static String uuidLiteral(SqlDialect dialect) {
        return switch (dialect.name()) {
            case "postgres" -> "'018f0000-0000-7000-8000-000000000001'";
            case "mysql" -> "UNHEX('018F0000000070008000000000000001')";
            default -> "X'018F0000000070008000000000000001'";
        };
    }

    /** Builds the timestamp index on a table that does not exist, so creating it fails. */
    private record MissingColumnIndexes(SqlDialect d) implements SqlDialect {
        @Override public @NotNull String createPlainIndexSql(@NotNull String tableName, @NotNull String indexName,
                                                             @NotNull List<String> columns) {
            if (!indexName.endsWith("_ts")) return d.createPlainIndexSql(tableName, indexName, columns);
            return d.createPlainIndexSql(tableName + "_missing", indexName, columns);
        }
        @Override public boolean isDuplicateIndex(@NotNull SQLException e) { return d.isDuplicateIndex(e); }
        @Override public boolean supportsCreateIndexIfNotExists() { return d.supportsCreateIndexIfNotExists(); }
        @Override public boolean rebuildsUuidIdColumns() { return d.rebuildsUuidIdColumns(); }
        @Override public boolean isBinaryColumnType(@NotNull String typeName) { return d.isBinaryColumnType(typeName); }
        @Override public @NotNull String name() { return d.name(); }
        @Override public @NotNull String createTableSql(@NotNull String t, @NotNull EncodeShape s) { return d.createTableSql(t, s); }
        @Override public @NotNull String createIndexSql(@NotNull String t, @NotNull IndexSpec s) { return d.createIndexSql(t, s); }
        @Override public @NotNull List<IndexCoverage.Existing> existingIndexes(@NotNull Connection c, @NotNull String t)
                throws SQLException { return d.existingIndexes(c, t); }
        @Override public @NotNull String columnTypeSql(@NotNull EncodeShape.FieldDef f) { return d.columnTypeSql(f); }
        @Override public @NotNull String quoteIdentifier(@NotNull String n) { return d.quoteIdentifier(n); }
        @Override public @NotNull String upsertSql(@NotNull String t, @NotNull EncodeShape s) { return d.upsertSql(t, s); }
    }
}
