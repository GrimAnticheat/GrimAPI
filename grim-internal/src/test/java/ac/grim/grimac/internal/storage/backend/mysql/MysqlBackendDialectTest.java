package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end round-trip for {@link MysqlBackend} against both server flavors,
 * driving real {@link MysqlEightDialect} (port 3306, mysql:8.4 fixture) and
 * {@link MariaDbDialect} (port 3307, mariadb:10.11 fixture) paths.
 * <p>
 * Skips per flavor if the fixture isn't reachable, so the test is a no-op on
 * machines without the per-zone test-data containers (CI runners, contributor
 * laptops). The fixture creds are the static values committed to
 * {@code /opt/grim-setup/test-data.conf} — they aren't secrets, the daemons
 * are reachable only inside the gluetun netns.
 * <p>
 * Both dialects MUST observe identical read-back semantics: same identity
 * upsert merging, case-insensitive name lookup, prefix search, session
 * latest-wins, settings UPSERT on composite PK, paged violation reads.
 */
class MysqlBackendDialectTest {

    private enum Flavor {
        MYSQL("mysql", 3306, "grim-test-mysql-root", MysqlEightDialect.class),
        MARIADB("mariadb", 3307, "grim-test-mariadb-root", MariaDbDialect.class);

        final String label;
        final int port;
        final String rootPassword;
        final Class<? extends MysqlDialect> expectedDialect;

        Flavor(String label, int port, String rootPassword, Class<? extends MysqlDialect> expectedDialect) {
            this.label = label;
            this.port = port;
            this.rootPassword = rootPassword;
            this.expectedDialect = expectedDialect;
        }

        String adminUrl() {
            return "jdbc:mysql://127.0.0.1:" + port + "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=2000";
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Flavor.class)
    @DisplayName("MySQL/MariaDB dialect — fresh schema + write/read/upsert-merge round-trip")
    void roundTrip(Flavor flavor, @TempDir Path tempDir) throws Exception {
        assumeTrue(reachable(flavor), () ->
                "Skipping; " + flavor.label + " fixture not reachable on 127.0.0.1:" + flavor.port);

        // Fresh DB so applyBaseline runs the v8 baseline. Migration path is
        // exercised separately in v7ToV8Migration() below.
        try (Connection admin = DriverManager.getConnection(flavor.adminUrl(), "root", flavor.rootPassword);
             Statement s = admin.createStatement()) {
            s.executeUpdate("DROP DATABASE IF EXISTS grim");
            s.executeUpdate("CREATE DATABASE grim CHARACTER SET utf8mb4");
        }

        MysqlBackendConfig cfg = new MysqlBackendConfig(
                "127.0.0.1", flavor.port, "grim", "root", flavor.rootPassword,
                "", 256, TableNames.DEFAULTS);
        MysqlBackend b = new MysqlBackend(cfg);
        b.init(ctx(tempDir));

        try {
            assertSame(flavor.expectedDialect, b.dialectForTest().getClass(),
                    "Wrong dialect picked for " + flavor.label);

            UUID player = UUID.randomUUID();
            UUID session = UUID.randomUUID();
            long t0 = 1_700_000_000_000L;

            // identity: two writes with bracketing timestamps; expect first_seen=min,
            // last_seen=max, current_name=second-write.
            StorageEventHandler<PlayerIdentityEvent> ih = b.eventHandlerFor(Categories.PLAYER_IDENTITY);
            ih.onEvent(newIdentity(player, "AlphaBravo", t0 + 100, t0 + 100), 0, false);
            ih.onEvent(newIdentity(player, "charlieDelta", t0, t0 + 500), 1, true);

            StorageEventHandler<SessionEvent> sh = b.eventHandlerFor(Categories.SESSION);
            sh.onEvent(newSession(session, player, "srv-1", t0, t0), 0, false);
            sh.onEvent(newSession(session, player, "srv-2", t0, t0 + 1000), 1, true);

            StorageEventHandler<ViolationEvent> vh = b.eventHandlerFor(Categories.VIOLATION);
            for (int i = 0; i < 5; i++) {
                ViolationEvent v = new ViolationEvent();
                v.sessionId(session).playerUuid(player).checkId(42 + i).vl(1.0 + i)
                        .occurredEpochMs(t0 + i).verbose("v" + i).verboseFormat(VerboseFormat.TEXT);
                vh.onEvent(v, i, i == 4);
            }

            StorageEventHandler<SettingEvent> seth = b.eventHandlerFor(Categories.SETTING);
            seth.onEvent(newSetting("hello", "world-1".getBytes(), t0), 0, false);
            seth.onEvent(newSetting("hello", "world-2".getBytes(), t0 + 1), 1, true);

            // --- reads ---
            Page<PlayerIdentity> idPage = b.read(Categories.PLAYER_IDENTITY, new Queries.GetPlayerIdentity(player));
            assertEquals(1, idPage.items().size(), "identity row count");
            PlayerIdentity id = idPage.items().get(0);
            assertEquals("charlieDelta", id.currentName(), "current_name = latest write");
            assertEquals(t0, id.firstSeenEpochMs(), "first_seen = min of writes");
            assertEquals(t0 + 500, id.lastSeenEpochMs(), "last_seen = max of writes");

            // The case-insensitive name path is the load-bearing one for the
            // MariaDB schema: it must hit the STORED generated column index,
            // not fall back to a full-table scan.
            Page<PlayerIdentity> byName = b.read(Categories.PLAYER_IDENTITY,
                    new Queries.GetPlayerIdentityByName("charliedelta"));
            assertEquals(1, byName.items().size(), "case-insensitive name lookup");

            Page<PlayerIdentity> byNameMixedCase = b.read(Categories.PLAYER_IDENTITY,
                    new Queries.GetPlayerIdentityByName("CHARLIEDELTA"));
            assertEquals(1, byNameMixedCase.items().size(), "case-insensitive name lookup (uppercase input)");

            Page<PlayerIdentity> prefix = b.read(Categories.PLAYER_IDENTITY,
                    Queries.listPlayersByNamePrefix("char", 25));
            assertEquals(1, prefix.items().size(), "prefix search finds the identity");

            Page<PlayerIdentity> prefixEmpty = b.read(Categories.PLAYER_IDENTITY,
                    Queries.listPlayersByNamePrefix("", 25));
            assertEquals(0, prefixEmpty.items().size(), "empty prefix returns empty page");

            Page<SessionRecord> sbi = b.read(Categories.SESSION, new Queries.GetSessionById(session));
            assertEquals(1, sbi.items().size(), "session row count");
            SessionRecord s = sbi.items().get(0);
            assertEquals("srv-2", s.serverName(), "session server_name = latest write");
            assertEquals(t0 + 1000, s.lastActivityEpochMs(), "session last_activity = latest write");

            Page<ViolationRecord> vs = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 10, null));
            assertEquals(5, vs.items().size(), "violation row count");

            Page<SettingRecord> sr = b.read(Categories.SETTING,
                    new Queries.GetSetting(SettingScope.SERVER, "grim-core", "hello"));
            assertEquals(1, sr.items().size(), "setting row count");
            assertEquals("world-2", new String(sr.items().get(0).value()), "setting value = latest write");
            assertEquals(t0 + 1, sr.items().get(0).updatedEpochMs(), "setting updated_at = latest write");

            assertEquals(5L, b.countViolationsInSession(session), "countViolationsInSession");
            assertEquals(1L, b.countSessionsByPlayer(player), "countSessionsByPlayer");

            Page<ViolationRecord> first = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 2, null));
            assertEquals(2, first.items().size(), "page 1 size");
            assertNotNull(first.nextCursor(), "page 1 has cursor");
        } finally {
            try { b.close(); } catch (BackendException ignore) {}
        }
    }

    /**
     * Pre-seeds a v7-shape MySQL database (functional index, no
     * {@code current_name_lower} column, schema_version=7), then boots
     * MysqlBackend and verifies the v7→v8 migration ran: the gen col exists
     * and is auto-populated, the new plain index is in place, the old
     * functional index is gone, schema_version is 8, and read queries
     * still work end-to-end.
     * <p>
     * MariaDB-only: skipped on MariaDB because no v7 MariaDB schema ever
     * existed (the dialect failed to initialise pre-this-PR; new MariaDB
     * deployments are born at v8 directly via the unified baseline).
     */
    @org.junit.jupiter.api.Test
    @DisplayName("MySQL v7→v8 migration — gen col + index swap on existing database")
    void v7ToV8Migration(@TempDir Path tempDir) throws Exception {
        Flavor flavor = Flavor.MYSQL;
        assumeTrue(reachable(flavor), "Skipping; mysql fixture not reachable");

        // Reset DB and pre-seed the v7 shape: functional index on
        // LOWER(current_name), no current_name_lower column, schema_version=7
        // in the meta table. Mirrors what an alpha v1.3.2.0 MySQL deployment
        // would look like before this PR landed.
        try (Connection admin = DriverManager.getConnection(flavor.adminUrl(), "root", flavor.rootPassword);
             Statement s = admin.createStatement()) {
            s.executeUpdate("DROP DATABASE IF EXISTS grim");
            s.executeUpdate("CREATE DATABASE grim CHARACTER SET utf8mb4");
            s.executeUpdate("USE grim");
            s.executeUpdate("CREATE TABLE grim_meta ("
                    + "id TINYINT PRIMARY KEY, schema_version INT NOT NULL, "
                    + "grim_core_version VARCHAR(64), initialized_at BIGINT NOT NULL, "
                    + "last_migration_at BIGINT NOT NULL, "
                    + "CONSTRAINT grim_meta_singleton CHECK (id = 0)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            s.executeUpdate("CREATE TABLE grim_checks ("
                    + "check_id INT AUTO_INCREMENT PRIMARY KEY, "
                    + "stable_key VARCHAR(255) NOT NULL UNIQUE, "
                    + "display VARCHAR(255), description VARCHAR(1024), "
                    + "introduced_version VARCHAR(64), removed_version VARCHAR(64), "
                    + "introduced_at BIGINT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            // The v7 players-table shape: functional idx, no gen col.
            s.executeUpdate("CREATE TABLE grim_players ("
                    + "uuid BINARY(16) PRIMARY KEY, current_name VARCHAR(32), "
                    + "first_seen BIGINT NOT NULL, last_seen BIGINT NOT NULL, "
                    + "INDEX idx_grim_players_name_lower ((LOWER(current_name)))"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            s.executeUpdate("CREATE TABLE grim_sessions ("
                    + "session_id BINARY(16) PRIMARY KEY, player_uuid BINARY(16) NOT NULL, "
                    + "server_name VARCHAR(64), started_at BIGINT NOT NULL, "
                    + "last_activity BIGINT NOT NULL, closed_at BIGINT NULL, "
                    + "grim_version VARCHAR(64), client_brand VARCHAR(64), "
                    + "client_version_pvn INT NOT NULL DEFAULT -1, "
                    + "server_version VARCHAR(64), replay_clips_json MEDIUMTEXT, "
                    + "INDEX idx_grim_sessions_player_started (player_uuid, started_at DESC)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            s.executeUpdate("CREATE TABLE grim_violations ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, session_id BINARY(16) NOT NULL, "
                    + "player_uuid BINARY(16) NOT NULL, check_id INT NOT NULL, "
                    + "vl DOUBLE NOT NULL, occurred_at BIGINT NOT NULL, "
                    + "verbose MEDIUMTEXT, verbose_format INT NOT NULL DEFAULT 0, "
                    + "metadata MEDIUMTEXT, "
                    + "INDEX idx_grim_violations_session_time (session_id, occurred_at), "
                    + "INDEX idx_grim_violations_player (player_uuid)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            s.executeUpdate("CREATE TABLE grim_settings ("
                    + "scope VARCHAR(16) NOT NULL, scope_key VARCHAR(128) NOT NULL, "
                    + "`key` VARCHAR(128) NOT NULL, value LONGBLOB NOT NULL, "
                    + "updated_at BIGINT NOT NULL, "
                    + "PRIMARY KEY (scope, scope_key, `key`)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            s.executeUpdate("INSERT INTO grim_meta (id, schema_version, grim_core_version, initialized_at, last_migration_at) "
                    + "VALUES (0, 7, 'v7-seed', 1, 1)");
            // Seed an identity row so we can verify the gen col gets backfilled.
            s.executeUpdate("INSERT INTO grim_players (uuid, current_name, first_seen, last_seen) "
                    + "VALUES (UNHEX('0000000000000000000000000000007F'), 'CamelCaseName', 100, 200)");
        }

        MysqlBackendConfig cfg = new MysqlBackendConfig(
                "127.0.0.1", flavor.port, "grim", "root", flavor.rootPassword,
                "", 256, TableNames.DEFAULTS);
        MysqlBackend b = new MysqlBackend(cfg);
        b.init(ctx(tempDir));

        try {
            // Inspect the post-migration shape directly.
            try (Connection c = DriverManager.getConnection(flavor.adminUrl() + "&database=grim", "root", flavor.rootPassword);
                 Statement s = c.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT schema_version FROM grim.grim_meta WHERE id = 0")) {
                    org.junit.jupiter.api.Assertions.assertTrue(rs.next());
                    assertEquals(8, rs.getInt(1), "schema_version bumped to 8");
                }
                try (ResultSet rs = s.executeQuery(
                        "SELECT current_name_lower FROM grim.grim_players WHERE uuid = UNHEX('0000000000000000000000000000007F')")) {
                    org.junit.jupiter.api.Assertions.assertTrue(rs.next());
                    assertEquals("camelcasename", rs.getString(1),
                            "STORED gen col backfilled the existing seed row");
                }
                // The post-migration index must be on current_name_lower (a real
                // column), not on the functional expression LOWER(current_name).
                int columnIndexEntries = 0;
                try (ResultSet rs = s.executeQuery(
                        "SHOW INDEX FROM grim.grim_players WHERE Key_name = 'idx_grim_players_name_lower'")) {
                    while (rs.next()) {
                        if ("current_name_lower".equals(rs.getString("Column_name"))) {
                            columnIndexEntries++;
                        }
                    }
                }
                assertEquals(1, columnIndexEntries,
                        "idx_grim_players_name_lower now indexes the column, not the LOWER() expression");
            }

            // End-to-end read after migration: existing seed row should be
            // findable via the case-insensitive name lookup.
            Page<PlayerIdentity> byName = b.read(Categories.PLAYER_IDENTITY,
                    new Queries.GetPlayerIdentityByName("camelcasename"));
            assertEquals(1, byName.items().size(), "post-migration name lookup finds seed row");
            assertEquals("CamelCaseName", byName.items().get(0).currentName());

            Page<PlayerIdentity> prefix = b.read(Categories.PLAYER_IDENTITY,
                    Queries.listPlayersByNamePrefix("camel", 25));
            assertEquals(1, prefix.items().size(), "post-migration prefix lookup hits the new index");
        } finally {
            try { b.close(); } catch (BackendException ignore) {}
        }
    }

    private static boolean reachable(Flavor f) {
        try (Connection c = DriverManager.getConnection(f.adminUrl(), "root", f.rootPassword)) {
            return c.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }

    private static PlayerIdentityEvent newIdentity(UUID uuid, String name, long firstSeen, long lastSeen) {
        PlayerIdentityEvent e = new PlayerIdentityEvent();
        e.uuid(uuid).currentName(name).firstSeenEpochMs(firstSeen).lastSeenEpochMs(lastSeen);
        return e;
    }

    private static SessionEvent newSession(UUID sessionId, UUID playerUuid, String serverName,
                                           long startedAt, long lastActivity) {
        SessionEvent s = new SessionEvent();
        s.sessionId(sessionId).playerUuid(playerUuid).serverName(serverName)
                .startedEpochMs(startedAt).lastActivityEpochMs(lastActivity)
                .grimVersion("test").clientBrand("vanilla")
                .clientVersion(772).serverVersionString("1.21.11");
        return s;
    }

    private static SettingEvent newSetting(String key, byte[] value, long updatedAt) {
        SettingEvent e = new SettingEvent();
        e.scope(SettingScope.SERVER).scopeKey("grim-core").key(key)
                .value(value).updatedEpochMs(updatedAt);
        return e;
    }

    private static BackendContext ctx(Path dataDir) {
        Logger log = Logger.getLogger("mysql-dialect-test");
        return new BackendContext() {
            @Override public BackendConfig config() { return null; }
            @Override public Logger logger() { return log; }
            @Override public Path dataDirectory() { return dataDir; }
        };
    }
}
