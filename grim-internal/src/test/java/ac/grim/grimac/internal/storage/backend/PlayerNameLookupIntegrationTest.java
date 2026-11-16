package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Looking a player up by name ignores case on every SQL database, and on MySQL/MariaDB
 * it uses the name index: those have no LOWER() expression index, so a LOWER(col)
 * comparison scanned the whole players table.
 */
@DisplayName("Player name lookups ignore case and use the name index")
class PlayerNameLookupIntegrationTest {

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "names.db")) {
            scenario(db);
        }
    }

    @Test
    void postgres() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.postgres()) {
            scenario(db);
            String table = lastTable;
            // A prefix with no match must be an index range, not a walk over every name (the database's
            // collation is not "C", so the LOWER() index alone cannot bound a LIKE prefix).
            try (Connection c = db.open(); Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("EXPLAIN SELECT * FROM \"" + table + "\" WHERE LOWER(\"current_name\")"
                     + " LIKE 'zz%' ESCAPE '!' ORDER BY LOWER(\"current_name\") LIMIT 21")) {
                StringBuilder plan = new StringBuilder();
                while (rs.next()) plan.append(rs.getString(1)).append('\n');
                String p = plan.toString();
                assertTrue(p.contains(table + "_by_name_prefix") && p.contains("Index Cond") && !p.contains("Seq Scan"),
                    db.label() + " does not bound the prefix with an index: " + p);
            }
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE \"" + table + "\"");
            }
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void mysql() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.mysql()) {
            scenario(db);
            String table = lastTable;
            try (Connection c = db.open(); Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("EXPLAIN SELECT * FROM `" + table + "` WHERE `current_name` = 'player_1500'")) {
                // Read every column: MySQL 9 may print the plan as a tree instead of a table.
                StringBuilder plan = new StringBuilder();
                while (rs.next()) {
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        plan.append(rs.getMetaData().getColumnLabel(i)).append('=').append(rs.getString(i)).append(' ');
                    }
                }
                String p = plan.toString();
                assertTrue(p.contains("by_name") && !p.contains("type=ALL") && !p.contains("Table scan"),
                    db.label() + " does not use the name index: " + p);
            }
            // A table whose name column compares case-sensitively (one that predates utf8mb4, or a server
            // mapping utf8mb4 to utf8mb4_bin) still finds mixed-case names.
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("ALTER TABLE `" + table + "` MODIFY `current_name` VARCHAR(512)"
                    + " CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL");
            }
            try (SqlTestDatabases.Target restarted = db.reopen()) {
                Entity<UUID, ?, PlayerIdentity> kind = V2BuiltinKinds.players();
                ((KindAdapter) restarted.backend().adapterFor(kind).orElseThrow()).ensureStore(StoreId.grim(table), kind);
                lookups(restarted, table);
            }
            try (Connection c = db.open(); Statement s = c.createStatement()) {
                s.executeUpdate("DROP TABLE `" + table + "`");
            }
        }
    }

    private static String lastTable;

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void scenario(SqlTestDatabases.Target db) throws Exception {
        Entity<UUID, ?, PlayerIdentity> kind = V2BuiltinKinds.players();
        KindAdapter adapter = (KindAdapter) db.backend().adapterFor(kind).orElseThrow();
        String table = SqlTestDatabases.uniqueTable("names_players");
        lastTable = table;
        StoreId id = StoreId.grim(table);
        adapter.ensureStore(id, kind);
        for (int i = 0; i < 2_000; i++) {
            adapter.execute(id, kind, new EntityOps.UpsertOp<>(Categories.PLAYER_IDENTITY,
                new PlayerIdentity(new UUID(7, i), "Player_" + i, 1L, 2L)));
        }
        lookups(db, table);
    }

    /** Exact and prefix lookups ignore case. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void lookups(SqlTestDatabases.Target db, String table) throws Exception {
        Entity<UUID, ?, PlayerIdentity> kind = V2BuiltinKinds.players();
        KindAdapter adapter = (KindAdapter) db.backend().adapterFor(kind).orElseThrow();
        StoreId id = StoreId.grim(table);
        Page<PlayerIdentity> exact = (Page<PlayerIdentity>) adapter.execute(id, kind,
            new EntityOps.FindByIndexOp<>(Categories.PLAYER_IDENTITY, "by_name", "pLAYER_1500", null, 1));
        assertEquals(List.of(new UUID(7, 1500)), exact.items().stream().map(PlayerIdentity::uuid).toList(), db.label());

        Page<PlayerIdentity> prefix = (Page<PlayerIdentity>) adapter.execute(id, kind,
            new EntityOps.PrefixIndexOp<>(Categories.PLAYER_IDENTITY, "by_name", "player_199", null, 20));
        assertEquals(11, prefix.items().size(), db.label() + ": Player_199 and Player_1990..1999");
    }
}
