package ac.grim.grimac.internal.storage.migrate;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.SqliteCheckPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LegacyMigratorTest {

    private static final Logger LOG = Logger.getLogger("LegacyMigratorTest");

    @Test
    void migratesV0CorpusIntoReconstructedSessions(@TempDir Path tmp) throws Exception {
        Path v0Path = tmp.resolve("v0.db");
        Path v1Path = tmp.resolve("v1.db");
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        V0Fixture.build(v0Path)
                .server("Prison")
                .check("Reach")
                .check("Timer")
                .grimVersion("2.3.59")
                .clientBrand("vanilla")
                .clientVersion("1.21.1")
                .serverVersion("Paper/1.21.1")
                // Alice — two sessions (gap between t=10000 and t=720000 = 12 min > 10 min).
                .violation(alice, 1, 1, 1, 1000L, 1.0, "alice#1")
                .violation(alice, 1, 2, 1, 2000L, 1.0, "alice#2")
                .violation(alice, 1, 1, 1, 10000L, 2.0, "alice#3 /gl 1")
                .violation(alice, 1, 1, 1, 720_000L, 1.0, "alice#4")
                .violation(alice, 1, 2, 1, 721_000L, 1.0, "alice#5")
                // Bob — one session.
                .violation(bob, 1, 2, 1, 1500L, 1.0, "bob#1")
                .violation(bob, 1, 2, 1, 3500L, 1.0, "bob#2")
                .close();

        SqliteBackend v1 = initV1(v1Path);
        CheckRegistry registry = new CheckRegistry(new SqliteCheckPersistence(v1.jdbcUrl()));
        registry.reload();
        V0Reader reader = new V0Reader("jdbc:sqlite:" + v0Path.toAbsolutePath());

        LegacyMigrator migrator = new LegacyMigrator(reader, v1, registry, 600_000L, LOG);
        LegacyMigrator.Result result = migrator.run(p -> {});

        assertEquals(3, result.sessionsWritten(), "2 for alice, 1 for bob");
        assertEquals(7, result.violationsWritten());
        assertFalse(result.resumed());

        Page<SessionRecord> aliceSessions = v1.read(Categories.SESSION, Queries.listSessionsByPlayer(alice, 10, null));
        assertEquals(2, aliceSessions.items().size());
        Page<SessionRecord> bobSessions = v1.read(Categories.SESSION, Queries.listSessionsByPlayer(bob, 10, null));
        assertEquals(1, bobSessions.items().size());

        // Bob's session violations reproduced (time-ordered, verbose stripped of /gl).
        UUID bobSession = bobSessions.items().get(0).sessionId();
        assertEquals(2, v1.countViolationsInSession(bobSession));
        Page<ViolationRecord> bobV = v1.read(Categories.VIOLATION, Queries.listViolationsInSession(bobSession, 10, null));
        assertEquals(2, bobV.items().size());
        assertEquals(1500L, bobV.items().get(0).occurredEpochMs());

        // Verify alice's first-session verbose had " /gl 1" stripped.
        Page<SessionRecord> alicePage = v1.read(Categories.SESSION, Queries.listSessionsByPlayer(alice, 10, null));
        UUID older = alicePage.items().get(1).sessionId();
        Page<ViolationRecord> olderV = v1.read(Categories.VIOLATION, Queries.listViolationsInSession(older, 10, null));
        assertTrue(olderV.items().stream().anyMatch(v -> "alice#3".equals(v.verbose())),
                "verbose should have /gl macro stripped");

        // Player identities backfilled.
        Page<PlayerIdentity> aliceId = v1.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(alice));
        assertEquals(1, aliceId.items().size());
        assertEquals(1000L, aliceId.items().get(0).firstSeenEpochMs());
        assertEquals(721_000L, aliceId.items().get(0).lastSeenEpochMs());

        v1.close();
    }

    @Test
    void resumeFromCheckpointWritesOnlyRemainder(@TempDir Path tmp) throws Exception {
        Path v0Path = tmp.resolve("v0.db");
        Path v1Path = tmp.resolve("v1.db");
        UUID alice = UUID.randomUUID();
        V0Fixture.build(v0Path)
                .server("Prison")
                .check("Reach")
                .grimVersion("2.3.59")
                .clientBrand("vanilla")
                .clientVersion("1.21.1")
                .serverVersion("Paper/1.21.1")
                .violation(alice, 1, 1, 1, 1000L, 1.0, "1")
                .violation(alice, 1, 1, 1, 2000L, 1.0, "2")
                .violation(alice, 1, 1, 1, 3000L, 1.0, "3")
                .violation(alice, 1, 1, 1, 4000L, 1.0, "4")
                .close();

        SqliteBackend v1 = initV1(v1Path);
        // Pre-seed grim_migration_state: pretend we already migrated v0 ids 1 and 2.
        // Use a fresh connection (WAL mode supports concurrent writers on SQLite here
        // because the backend's write connection is idle).
        try (Connection c = DriverManager.getConnection(v1.jdbcUrl());
             Statement s = c.createStatement()) {
            s.executeUpdate("INSERT INTO grim_migration_state(id, last_migrated_violation_id, state, started_at, completed_at) "
                    + "VALUES (0, 2, 'IN_PROGRESS', 0, 0)");
        }
        CheckRegistry registry = new CheckRegistry(new SqliteCheckPersistence(v1.jdbcUrl()));
        registry.reload();
        V0Reader reader = new V0Reader("jdbc:sqlite:" + v0Path.toAbsolutePath());

        LegacyMigrator migrator = new LegacyMigrator(reader, v1, registry, 600_000L, LOG);
        LegacyMigrator.Result result = migrator.run(p -> {});
        assertTrue(result.resumed());
        assertEquals(2, result.violationsWritten(), "only v0 ids 3 and 4 processed");

        v1.close();
    }

    private SqliteBackend initV1(Path v1Path) throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults(v1Path.getFileName().toString());
        SqliteBackend v1 = new SqliteBackend(cfg);
        v1.init(new TestContext(cfg, LOG, v1Path.getParent()));
        return v1;
    }

    private record TestContext(BackendConfig config, Logger logger, Path dataDirectory) implements BackendContext {}

    /** Minimal writable builder for a v0 SQLite fixture matching the 2.0 schema. */
    private static final class V0Fixture implements AutoCloseable {
        private final Connection c;
        private final Map<String, Integer> servers = new HashMap<>();
        private final Map<String, Integer> checks = new HashMap<>();
        private final Map<String, Integer> grimVersions = new HashMap<>();
        private final Map<String, Integer> brands = new HashMap<>();
        private final Map<String, Integer> clientVs = new HashMap<>();
        private final Map<String, Integer> serverVs = new HashMap<>();

        static V0Fixture build(Path path) throws Exception {
            Class.forName("org.sqlite.JDBC");
            Connection c = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath());
            try (Statement s = c.createStatement()) {
                s.executeUpdate("CREATE TABLE grim_history_servers (id INTEGER PRIMARY KEY AUTOINCREMENT, server_name TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_check_names (id INTEGER PRIMARY KEY AUTOINCREMENT, check_name_string TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, grim_version_string TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_client_brands (id INTEGER PRIMARY KEY AUTOINCREMENT, client_brand_string TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_client_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, client_version_string TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_server_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, server_version_string TEXT UNIQUE)");
                s.executeUpdate("CREATE TABLE grim_history_violations ("
                        + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                        + "server_id INTEGER, uuid BLOB, check_name_id INTEGER, "
                        + "verbose TEXT, vl REAL, created_at INTEGER, "
                        + "grim_version_id INTEGER, client_brand_id INTEGER, "
                        + "client_version_id INTEGER, server_version_id INTEGER)");
            }
            return new V0Fixture(c);
        }

        private V0Fixture(Connection c) { this.c = c; }

        V0Fixture server(String name) throws Exception {
            int id = ensure("grim_history_servers", "server_name", servers, name);
            return this;
        }
        V0Fixture check(String name) throws Exception {
            ensure("grim_history_check_names", "check_name_string", checks, name);
            return this;
        }
        V0Fixture grimVersion(String v) throws Exception {
            ensure("grim_history_versions", "grim_version_string", grimVersions, v);
            return this;
        }
        V0Fixture clientBrand(String v) throws Exception {
            ensure("grim_history_client_brands", "client_brand_string", brands, v);
            return this;
        }
        V0Fixture clientVersion(String v) throws Exception {
            ensure("grim_history_client_versions", "client_version_string", clientVs, v);
            return this;
        }
        V0Fixture serverVersion(String v) throws Exception {
            ensure("grim_history_server_versions", "server_version_string", serverVs, v);
            return this;
        }

        V0Fixture violation(UUID player, int serverId, int checkId, int grimVId,
                            long createdAt, double vl, String verbose) throws Exception {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO grim_history_violations(server_id, uuid, check_name_id, verbose, vl, created_at, "
                            + "grim_version_id, client_brand_id, client_version_id, server_version_id) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setInt(1, serverId);
                ps.setBytes(2, uuidBytes(player));
                ps.setInt(3, checkId);
                ps.setString(4, verbose);
                ps.setDouble(5, vl);
                ps.setLong(6, createdAt);
                ps.setInt(7, grimVId);
                ps.setInt(8, 1);
                ps.setInt(9, 1);
                ps.setInt(10, 1);
                ps.executeUpdate();
            }
            return this;
        }

        private int ensure(String table, String col, Map<String, Integer> cache, String value) throws Exception {
            if (cache.containsKey(value)) return cache.get(value);
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + table + "(" + col + ") VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, value);
                ps.executeUpdate();
                try (var keys = ps.getGeneratedKeys()) {
                    int id = keys.next() ? keys.getInt(1) : -1;
                    cache.put(value, id);
                    return id;
                }
            }
        }

        private static byte[] uuidBytes(UUID uuid) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            return bb.array();
        }

        @Override public void close() throws Exception { c.close(); }
    }

    @SuppressWarnings("unused")
    private static List<List<Object>> nothing() { return List.of(); }
}
