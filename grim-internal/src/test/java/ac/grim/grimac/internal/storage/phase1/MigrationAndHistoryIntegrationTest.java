package ac.grim.grimac.internal.storage.phase1;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.history.HistoryService;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackend;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.SqliteCheckPersistence;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import ac.grim.grimac.internal.storage.history.HistoryServiceImpl;
import ac.grim.grimac.internal.storage.migrate.LegacyMigrator;
import ac.grim.grimac.internal.storage.migrate.V0Reader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-1 exit-criteria integration: build a v0 fixture with many violations,
 * migrate to v1, query through HistoryServiceImpl, confirm structured data
 * returned for migrated content.
 */
final class MigrationAndHistoryIntegrationTest {

    private static final Logger LOG = Logger.getLogger("Phase1IntegrationTest");

    @Test
    void migrateV0ThenListAndDetailSessions(@TempDir Path tmp) throws Exception {
        Path v0Path = tmp.resolve("violations.sqlite");
        Path v1Path = tmp.resolve("history.v1.db");

        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        buildV0Fixture(v0Path, alice, bob);

        SqliteBackendConfig cfg = SqliteBackendConfig.defaults(v1Path.getFileName().toString());
        SqliteBackend v1 = new SqliteBackend(cfg);
        try {
            v1.init(new TestContext(cfg, LOG, v1Path.getParent()));

            CheckRegistry registry = new CheckRegistry(new SqliteCheckPersistence(v1.jdbcUrl()));
            registry.reload();
            V0Reader reader = new V0Reader("jdbc:sqlite:" + v0Path.toAbsolutePath());
            LegacyMigrator migrator = new LegacyMigrator(reader, v1, registry, 600_000L, LOG);
            LegacyMigrator.Result result = migrator.run(x -> {});

            assertTrue(result.sessionsWritten() >= 2, "at least 2 sessions reconstructed");
            assertTrue(result.violationsWritten() >= 6, "at least 6 violations migrated");
            assertFalse(result.resumed());

            Map<Category<?>, Backend> routing = Map.of(
                    Categories.VIOLATION, v1,
                    Categories.SESSION, v1,
                    Categories.PLAYER_IDENTITY, v1,
                    Categories.SETTING, v1);
            DataStoreImpl store = new DataStoreImpl(new CategoryRouter(routing), WritePathConfig.defaults(), LOG);
            store.start();
            try {
                HistoryService history = new HistoryServiceImpl(store, registry, 10, 30_000L);

                Page<SessionSummary> listing = history
                        .listSessions(alice, null, 10)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertNotNull(listing);
                assertEquals(2, listing.items().size(), "alice should have two session summaries");
                for (SessionSummary summary : listing.items()) {
                    assertEquals(alice, summary.playerUuid());
                    assertTrue(summary.violationCount() > 0);
                }

                UUID firstSessionId = listing.items().get(0).sessionId();
                SessionDetail detail = history.getSessionDetail(alice, firstSessionId)
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertNotNull(detail);
                assertEquals(firstSessionId, detail.sessionId());
                assertFalse(detail.violations().isEmpty());
            } finally {
                store.flushAndClose(1_000);
            }
        } finally {
            v1.close();
        }
    }

    private void buildV0Fixture(Path path, UUID alice, UUID bob) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath())) {
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
                s.executeUpdate("INSERT INTO grim_history_servers(server_name) VALUES ('Prison')");
                s.executeUpdate("INSERT INTO grim_history_check_names(check_name_string) VALUES ('Reach'),('Timer')");
                s.executeUpdate("INSERT INTO grim_history_versions(grim_version_string) VALUES ('2.3.59')");
                s.executeUpdate("INSERT INTO grim_history_client_brands(client_brand_string) VALUES ('vanilla')");
                s.executeUpdate("INSERT INTO grim_history_client_versions(client_version_string) VALUES ('1.21.1')");
                s.executeUpdate("INSERT INTO grim_history_server_versions(server_version_string) VALUES ('Paper/1.21.1')");
            }
            insertV(c, alice, 1, 1, 1000);
            insertV(c, alice, 1, 2, 2000);
            insertV(c, alice, 1, 1, 10_000);
            insertV(c, alice, 1, 1, 720_000);    // gap > 10 min from previous — new session
            insertV(c, alice, 1, 2, 721_000);
            insertV(c, bob, 1, 2, 1500);
            insertV(c, bob, 1, 2, 3500);
        }
    }

    private void insertV(Connection c, UUID player, int server, int check, long t) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO grim_history_violations(server_id, uuid, check_name_id, verbose, vl, created_at, "
                        + "grim_version_id, client_brand_id, client_version_id, server_version_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, server);
            ps.setBytes(2, uuidBytes(player));
            ps.setInt(3, check);
            ps.setString(4, "v");
            ps.setDouble(5, 1.0);
            ps.setLong(6, t);
            ps.setInt(7, 1);
            ps.setInt(8, 1);
            ps.setInt(9, 1);
            ps.setInt(10, 1);
            ps.executeUpdate();
        }
    }

    private static byte[] uuidBytes(UUID u) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(u.getMostSignificantBits());
        bb.putLong(u.getLeastSignificantBits());
        return bb.array();
    }

    private record TestContext(BackendConfig config, Logger logger, Path dataDirectory) implements BackendContext {}
}
