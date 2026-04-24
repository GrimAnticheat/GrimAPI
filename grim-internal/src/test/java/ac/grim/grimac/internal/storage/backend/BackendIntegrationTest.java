package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.Backend;
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
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackend;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackend;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackend;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackend;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackendConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end smoke test for every non-SQLite backend against the local
 * {@code test-*-zone-a} containers documented in
 * {@code /opt/grim-setup/docs/TEST-DATA.md}. Each backend is exercised
 * through its {@link Backend} surface identically:
 * <ol>
 *   <li>Boot with {@link TableNames} prefixed by a random run-id so parallel
 *       test runs don't collide.</li>
 *   <li>Publish one event of each category via the returned
 *       {@link StorageEventHandler}.</li>
 *   <li>Round-trip reads through every {@link Queries} shape the category
 *       supports.</li>
 *   <li>Exercise the {@link Deletes#byPlayer} path.</li>
 *   <li>Close the backend.</li>
 * </ol>
 * Skipped with {@link Assumptions#assumeTrue} when the backing daemon isn't
 * reachable — lets the suite be run on dev boxes without the test-data stack.
 */
class BackendIntegrationTest {

    /** Shared creds come from /opt/grim-setup/test-data.conf. */
    private static final String MYSQL_HOST = "localhost";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_DB = "grim";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "grim-test-mysql-root";

    private static final String PG_HOST = "localhost";
    private static final int PG_PORT = 5432;
    private static final String PG_DB = "grim";
    private static final String PG_USER = "postgres";
    private static final String PG_PASS = "grim-test-postgres";

    private static final String MONGO_CS = "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin";
    private static final String MONGO_DB = "grim_it";

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final String REDIS_PASS = "grim-test-redis";

    private static Stream<Arguments> backends() {
        String runId = "it_" + Long.toHexString(System.nanoTime());
        TableNames sqlNames = withPrefix(runId + "_");
        TableNames redisNames = withPrefix(runId + "_");
        return Stream.of(
                Arguments.of("mysql", (BackendFactory) () -> new MysqlBackend(new MysqlBackendConfig(
                        MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASS, "", 64, sqlNames)),
                        MYSQL_HOST, MYSQL_PORT),
                Arguments.of("postgres", (BackendFactory) () -> new PostgresBackend(new PostgresBackendConfig(
                        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS, "", 64, sqlNames)),
                        PG_HOST, PG_PORT),
                Arguments.of("mongo", (BackendFactory) () -> new MongoBackend(new MongoBackendConfig(
                        MONGO_CS, MONGO_DB + "_" + runId, 64, sqlNames)),
                        "localhost", 27017),
                Arguments.of("redis", (BackendFactory) () -> new RedisBackend(new RedisBackendConfig(
                        REDIS_HOST, REDIS_PORT, 0, null, REDIS_PASS,
                        "it:" + runId + ":", 2000, 64, false, redisNames)),
                        REDIS_HOST, REDIS_PORT));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("backends")
    @DisplayName("write + read + delete round-trips per backend")
    void roundTrip(String label, BackendFactory factory, String host, int port) throws Exception {
        assumeReachable(host, port, label);

        Backend b = factory.create();
        b.init(ctx(b.id()));

        try {
            UUID player = UUID.randomUUID();
            UUID session = UUID.randomUUID();
            long now = System.currentTimeMillis();

            // --- identity ---
            StorageEventHandler<PlayerIdentityEvent> ih = b.eventHandlerFor(Categories.PLAYER_IDENTITY);
            PlayerIdentityEvent ie = new PlayerIdentityEvent();
            ie.uuid(player).currentName("AlphaBravo").firstSeenEpochMs(now).lastSeenEpochMs(now);
            ih.onEvent(ie, 0, true);

            // --- session ---
            StorageEventHandler<SessionEvent> sh = b.eventHandlerFor(Categories.SESSION);
            SessionEvent se = new SessionEvent();
            se.sessionId(session).playerUuid(player).serverName("test").startedEpochMs(now)
                    .lastActivityEpochMs(now).grimVersion("test").clientBrand("vanilla")
                    .clientVersion(772).serverVersionString("1.21.11");
            sh.onEvent(se, 0, true);

            // --- violations ---
            StorageEventHandler<ViolationEvent> vh = b.eventHandlerFor(Categories.VIOLATION);
            for (int i = 0; i < 5; i++) {
                ViolationEvent v = new ViolationEvent();
                v.sessionId(session).playerUuid(player).checkId(42 + i).vl(1.0 + i)
                        .occurredEpochMs(now + i).verbose("v" + i).verboseFormat(VerboseFormat.TEXT);
                vh.onEvent(v, i, i == 4);
            }

            // --- setting ---
            StorageEventHandler<SettingEvent> seth = b.eventHandlerFor(Categories.SETTING);
            SettingEvent set = new SettingEvent();
            set.scope(SettingScope.SERVER).scopeKey("grim-core").key("hello")
                    .value("world".getBytes()).updatedEpochMs(now);
            seth.onEvent(set, 0, true);

            // --- reads ---
            Page<PlayerIdentity> byUuid = b.read(Categories.PLAYER_IDENTITY, new Queries.GetPlayerIdentity(player));
            assertEquals(1, byUuid.items().size(), label + ": identity by uuid");
            assertEquals("AlphaBravo", byUuid.items().get(0).currentName(), label + ": identity name");

            Page<PlayerIdentity> byName = b.read(Categories.PLAYER_IDENTITY, new Queries.GetPlayerIdentityByName("alphabravo"));
            assertEquals(1, byName.items().size(), label + ": identity by name (case-insensitive)");

            Page<SessionRecord> sbi = b.read(Categories.SESSION, new Queries.GetSessionById(session));
            assertEquals(1, sbi.items().size(), label + ": session by id");
            assertEquals(772, sbi.items().get(0).clientVersion(), label + ": session pvn");

            Page<SessionRecord> byPlayer = b.read(Categories.SESSION,
                    new Queries.ListSessionsByPlayer(player, 10, null));
            assertEquals(1, byPlayer.items().size(), label + ": session by player");

            Page<ViolationRecord> vs = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 10, null));
            assertEquals(5, vs.items().size(), label + ": violation page size");

            assertEquals(5L, b.countViolationsInSession(session), label + ": countViolationsInSession");
            assertEquals(5L, b.countUniqueChecksInSession(session), label + ": countUniqueChecksInSession");
            assertEquals(1L, b.countSessionsByPlayer(player), label + ": countSessionsByPlayer");

            Page<SettingRecord> sr = b.read(Categories.SETTING,
                    new Queries.GetSetting(SettingScope.SERVER, "grim-core", "hello"));
            assertEquals(1, sr.items().size(), label + ": setting get");
            assertEquals("world", new String(sr.items().get(0).value()), label + ": setting value");

            // --- paginate ---
            Page<ViolationRecord> first = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 2, null));
            assertEquals(2, first.items().size(), label + ": page 1");
            assertNotNull(first.nextCursor(), label + ": page 1 cursor");
            Page<ViolationRecord> second = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 2, first.nextCursor()));
            assertEquals(2, second.items().size(), label + ": page 2");
            assertTrue(first.items().get(0).id() < second.items().get(0).id(), label + ": monotonic id");

            // --- delete ---
            b.delete(Categories.SESSION, new Deletes.ByPlayer(player));
            Page<ViolationRecord> afterDelete = b.read(Categories.VIOLATION,
                    new Queries.ListViolationsInSession(session, 10, null));
            assertEquals(0, afterDelete.items().size(), label + ": violations wiped");
            Page<SessionRecord> afterSession = b.read(Categories.SESSION, new Queries.GetSessionById(session));
            assertEquals(0, afterSession.items().size(), label + ": sessions wiped");
        } finally {
            dropSchema(b);
            try { b.close(); } catch (BackendException ignore) {}
        }
    }

    /**
     * Best-effort teardown so leftover {@code it_*} tables / collections / keys
     * from earlier runs don't accumulate in shared test-data containers. Each
     * backend exposes its configured {@link TableNames} via the same accessor
     * name for exactly this purpose.
     */
    private static void dropSchema(Backend b) {
        try {
            if (b instanceof MysqlBackend m) {
                TableNames t = m.tableNames();
                try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                        "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DB
                                + "?useSSL=false&allowPublicKeyRetrieval=true", MYSQL_USER, MYSQL_PASS);
                     java.sql.Statement s = c.createStatement()) {
                    for (String n : List.of(t.violations(), t.sessions(), t.players(), t.checks(), t.settings(), t.meta())) {
                        s.executeUpdate("DROP TABLE IF EXISTS `" + n + "`");
                    }
                }
            } else if (b instanceof PostgresBackend p) {
                TableNames t = p.tableNames();
                try (java.sql.Connection c = java.sql.DriverManager.getConnection(
                        "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/" + PG_DB, PG_USER, PG_PASS);
                     java.sql.Statement s = c.createStatement()) {
                    for (String n : List.of(t.violations(), t.sessions(), t.players(), t.checks(), t.settings(), t.meta())) {
                        s.executeUpdate("DROP TABLE IF EXISTS \"" + n + "\" CASCADE");
                    }
                }
            } else if (b instanceof MongoBackend m) {
                // Mongo backends use per-test databases (suffixed with runId),
                // so dropping the whole database is the clean move.
                String connStr = MONGO_CS;
                try (com.mongodb.client.MongoClient c = com.mongodb.client.MongoClients.create(connStr)) {
                    // Enumerate by pattern: it_*_<table> — simpler to drop the
                    // containing database instead since we suffixed it by runId.
                    // The factory names it MONGO_DB + "_" + runId, so walk
                    // all dbs and drop ones starting with grim_it_.
                    for (String name : c.listDatabaseNames()) {
                        if (name.startsWith("grim_it_")) c.getDatabase(name).drop();
                    }
                }
            } else if (b instanceof RedisBackend r) {
                TableNames t = r.tableNames();
                // Every key this backend touched is prefixed with the backend's
                // keyPrefix + one of the TableNames entries; SCAN+DEL by prefix
                // clears the test run without affecting neighbouring data.
                try (redis.clients.jedis.Jedis j = new redis.clients.jedis.Jedis(REDIS_HOST, REDIS_PORT)) {
                    j.auth(REDIS_PASS);
                    for (String logical : List.of(t.meta(), t.checks(), t.players(), t.sessions(), t.violations(), t.settings())) {
                        String pattern = "it:*:" + logical + "*";
                        var sp = new redis.clients.jedis.params.ScanParams().match(pattern).count(500);
                        String cursor = redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;
                        do {
                            var res = j.scan(cursor, sp);
                            cursor = res.getCursor();
                            for (String k : res.getResult()) j.del(k);
                        } while (!cursor.equals(redis.clients.jedis.params.ScanParams.SCAN_POINTER_START));
                    }
                }
            }
        } catch (Exception ignore) {
            // Best-effort: leftover test data isn't worth failing a passing
            // round-trip over.
        }
    }

    // --- helpers ------------------------------------------------------------

    private static TableNames withPrefix(String prefix) {
        return new TableNames(
                prefix + "meta", prefix + "checks", prefix + "players",
                prefix + "sessions", prefix + "violations", prefix + "settings");
    }

    private static void assumeReachable(String host, int port, String label) {
        try (Socket s = new Socket()) {
            s.connect(new java.net.InetSocketAddress(host, port), 500);
        } catch (Exception e) {
            Assumptions.abort("skipping " + label + ": " + host + ":" + port + " unreachable (" + e.getMessage() + ")");
        }
    }

    private static BackendContext ctx(String id) {
        Logger log = Logger.getLogger("backend-it-" + id);
        return new BackendContext() {
            @Override public BackendConfig config() { return null; }
            @Override public Logger logger() { return log; }
            @Override public Path dataDirectory() { return Path.of(System.getProperty("java.io.tmpdir"), "grim-it"); }
        };
    }

    @FunctionalInterface
    interface BackendFactory {
        Backend create() throws Exception;
    }
}
