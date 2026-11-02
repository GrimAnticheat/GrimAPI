package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.Codec;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.event.ServerStartupEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipRenewResult;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.CounterEvent;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.ops.CounterOps;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoBackendV2;
import ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig;
import ac.grim.grimac.internal.storage.backend.mysql.v2.MysqlBackendV2;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.postgres.v2.PostgresBackendV2;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackendConfig;
import ac.grim.grimac.internal.storage.backend.redis.v2.RedisBackendV2;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.backend.sqlite.v2.SqliteBackendV2;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.util.UuidV7;
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for v2 backends against real test containers.
 * Each test verifies: init → ensureStore → write via writeHandler →
 * read via GetByIdOp → verify round-trip. Gated on connectivity.
 */
@DisplayName("V2 backend integration (real containers)")
class V2BackendIntegrationTest {

    private static final Logger LOG = Logger.getLogger("V2BackendIntegrationTest");

    @Test @DisplayName("SQLite v2: entity indexes")
    void sqlite(@TempDir Path tempDir) throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("data/v2-integ.db");
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        try {
            backend.init(ctx(cfg, tempDir));
            assertTrue(Files.isDirectory(tempDir.resolve("data")),
                    "SQLite backend creates missing parent directories");
            exerciseEntityIndexes(backend, "v2_integ_players_sqlite", "v2_integ_sessions_sqlite",
                    "v2_integ_startups_sqlite");
            exerciseSettingsKv(backend, "v2_integ_settings_sqlite");
            exerciseCounter(backend, "v2_integ_counters_sqlite");
            exerciseOwnership(backend, "v2_integ_ownership_sqlite");
            exerciseSetIfSentinel(backend, "v2_integ_set_if_sentinel_sqlite");
            Path db = tempDir.resolve("data/v2-integ.db").toAbsolutePath();
            exerciseViolationIndexes(backend, "v2_integ_violations_sqlite",
                    () -> DriverManager.getConnection("jdbc:sqlite:" + db), "DROP INDEX \"%1$s\"", false);
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("SQLite v2 legacy dialect: entity/KV/counter writes")
    void sqliteLegacy(@TempDir Path tempDir) throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("data/v2-legacy-integ.db");
        SqliteBackendV2 backend = new SqliteBackendV2(cfg, SqliteDialect.legacyForTest());
        try {
            backend.init(ctx(cfg, tempDir));
            exerciseEntityIndexes(backend, "v2_legacy_players_sqlite", "v2_legacy_sessions_sqlite",
                    "v2_legacy_startups_sqlite");
            exerciseSettingsKv(backend, "v2_legacy_settings_sqlite");
            exerciseCounter(backend, "v2_legacy_counters_sqlite");
            exerciseOwnership(backend, "v2_legacy_ownership_sqlite");
            exerciseSetIfSentinel(backend, "v2_legacy_set_if_sentinel_sqlite");
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Postgres v2: entity write + read")
    void postgres() throws Exception {
        String host = System.getProperty("grim.test.postgres.host", "localhost");
        int port = Integer.getInteger("grim.test.postgres.port", 5432);
        assumeReachable(host, port);
        PostgresBackendConfig cfg = new PostgresBackendConfig(
            host, port,
            System.getProperty("grim.test.postgres.database", "grim"),
            System.getProperty("grim.test.postgres.user", "postgres"),
            System.getProperty("grim.test.postgres.password", "grim-test-postgres"),
            "", 256, TableNames.DEFAULTS);
        PostgresBackendV2 backend = new PostgresBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_pg", "v2_integ_sessions_pg",
                    "v2_integ_startups_pg");
            exerciseSetIfSentinel(backend, "v2_integ_set_if_sentinel_pg");
            exerciseViolationIndexes(backend, "v2_integ_violations_pg",
                    () -> DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + cfg.database(),
                            cfg.user(), cfg.password()),
                    "DROP INDEX \"%1$s\"", false);
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Redis v2: entity indexes")
    void redis() throws Exception {
        String host = System.getProperty("grim.test.redis.host", "localhost");
        int port = Integer.getInteger("grim.test.redis.port", 6379);
        assumeReachable(host, port);
        RedisBackendConfig cfg = new RedisBackendConfig(
            host, port, 0, null, System.getProperty("grim.test.redis.password", "grim-test-redis"),
            "v2integ:", 2000, 256, false, TableNames.DEFAULTS);
        RedisBackendV2 backend = new RedisBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_redis", "v2_integ_sessions_redis",
                    "v2_integ_startups_redis");
            exerciseSetIfSentinel(backend, "v2_integ_set_if_sentinel_redis");
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("MySQL v2: entity indexes")
    void mysql() throws Exception {
        String host = System.getProperty("grim.test.mysql.host", "localhost");
        int port = Integer.getInteger("grim.test.mysql.port", 3306);
        assumeReachable(host, port);
        MysqlBackendConfig cfg = new MysqlBackendConfig(
            host, port,
            System.getProperty("grim.test.mysql.database", "grim"),
            System.getProperty("grim.test.mysql.user", "grim"),
            System.getProperty("grim.test.mysql.password", "grim-test-mysql"),
            "", 256, TableNames.DEFAULTS);
        MysqlBackendV2 backend = new MysqlBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_mysql", "v2_integ_sessions_mysql",
                    "v2_integ_startups_mysql");
            exerciseSetIfSentinel(backend, "v2_integ_set_if_sentinel_mysql");
            exerciseViolationIndexes(backend, "v2_integ_violations_mysql",
                    () -> DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + cfg.database(),
                            cfg.user(), cfg.password()),
                    "DROP INDEX `%1$s` ON `%2$s`", true);
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Mongo v2: entity indexes")
    void mongo() throws Exception {
        String uri = System.getProperty("grim.test.mongo.uri",
                "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin");
        ServerAddress address = new ServerAddress(new ConnectionString(uri).getHosts().get(0));
        assumeReachable(address.getHost(), address.getPort());
        MongoBackendConfig cfg = new MongoBackendConfig(uri, "v2_integration_test", 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_mongo", "v2_integ_sessions_mongo",
                    "v2_integ_startups_mongo");
            exerciseSetIfSentinel(backend, "v2_integ_set_if_sentinel_mongo");
        } finally {
            backend.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exerciseViolationIndexes(BackendV2 backend, String storeName, JdbcConnector jdbc,
                                          String dropIndexSql, boolean raceInitializers) throws Exception {
        String table = storeName + "_" + Long.toHexString(System.nanoTime());
        StoreId store = StoreId.grim(table);
        EventStream kind = V2BuiltinKinds.violations();
        KindAdapter adapter = backend.adapterFor(kind)
                .orElseThrow(() -> new AssertionError(backend.id() + " has no EventStream adapter"));
        Map<String, String> expected = new TreeMap<>(Map.of(
                "idx_" + table + "_session_id", "session_id",
                "idx_" + table + "_player_uuid", "player_uuid",
                "idx_" + table + "_check_id", "check_id",
                "idx_" + table + "_ts", "occurred_at"));
        List<LogRecord> warnings = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override public void publish(LogRecord record) {
                if (record.getLevel() == Level.WARNING) warnings.add(record);
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        LOG.addHandler(capture);
        try {
            adapter.ensureStore(store, kind);
            assertEquals(expected, violationIndexes(jdbc, table), backend.id() + ": fresh table indexes");

            UUID session = UUID.randomUUID();
            UUID player = UUID.randomUUID();
            long ts = 1_700_000_000_000L;
            var handler = adapter.writeHandler(store, kind, Categories.VIOLATION);
            for (int i = 0; i < 3; i++) {
                handler.onEvent(new ViolationEvent()
                        .id(UuidV7.fromTimestampMs(ts, i))
                        .sessionId(session)
                        .playerUuid(player)
                        .checkId(7)
                        .vl(1.0)
                        .occurredEpochMs(ts), 0L, i == 2);
            }

            adapter.ensureStore(store, kind);
            assertEquals(expected, violationIndexes(jdbc, table), backend.id() + ": repeated ensureStore");

            try (Connection c = jdbc.open(); Statement s = c.createStatement()) {
                s.executeUpdate(String.format(dropIndexSql, "idx_" + table + "_check_id", table));
                s.executeUpdate(String.format(dropIndexSql, "idx_" + table + "_ts", table));
            }
            adapter.ensureStore(store, kind);
            assertEquals(expected, violationIndexes(jdbc, table), backend.id() + ": partial schema repaired");

            if (raceInitializers) {
                try (Connection c = jdbc.open(); Statement s = c.createStatement()) {
                    for (String index : expected.keySet()) s.executeUpdate(String.format(dropIndexSql, index, table));
                }
                assertTrue(violationIndexes(jdbc, table).isEmpty(), backend.id() + ": indexes dropped before race");
                CyclicBarrier start = new CyclicBarrier(2);
                ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                    List<Future<?>> runs = new ArrayList<>();
                    for (int i = 0; i < 2; i++) {
                        runs.add(pool.submit(() -> {
                            start.await();
                            adapter.ensureStore(store, kind);
                            return null;
                        }));
                    }
                    for (Future<?> run : runs) run.get(60, TimeUnit.SECONDS);
                } finally {
                    pool.shutdownNow();
                }
                assertEquals(expected, violationIndexes(jdbc, table), backend.id() + ": concurrent initializers");
            }

            assertEquals(3L, (long) adapter.execute(store, kind,
                    new EventStreamOps.CountOp(Categories.VIOLATION, "player_uuid", player)),
                    backend.id() + ": rows survive index repair");
            Page<?> bySession = (Page<?>) adapter.execute(store, kind,
                    new EventStreamOps.PageOp<>(Categories.VIOLATION, "session_id", session, null, 10));
            assertEquals(3, bySession.items().size(), backend.id() + ": page by session");
            Page<?> byTime = (Page<?>) adapter.execute(store, kind,
                    new EventStreamOps.RangeByTimeOp<>(Categories.VIOLATION, ts - 1, ts + 1, null, 10));
            assertEquals(3, byTime.items().size(), backend.id() + ": range by time");
            assertTrue(warnings.isEmpty(), backend.id() + ": no index warnings, got " + warnings.stream()
                    .map(r -> r.getMessage() + " / " + r.getThrown()).toList());
        } finally {
            LOG.removeHandler(capture);
            adapter.dropStore(store, kind);
        }
    }

    private interface JdbcConnector {
        Connection open() throws SQLException;
    }

    private static Map<String, String> violationIndexes(JdbcConnector jdbc, String table)
            throws SQLException {
        Map<String, String> out = new TreeMap<>();
        try (Connection c = jdbc.open();
             ResultSet rs = c.getMetaData().getIndexInfo(c.getCatalog(), c.getSchema(), table, false, false)) {
            while (rs.next()) {
                String name = rs.getString("INDEX_NAME");
                if (name == null || !name.startsWith("idx_")) continue;
                assertTrue(rs.getBoolean("NON_UNIQUE"), name + " is non-unique");
                assertNull(out.put(name, rs.getString("COLUMN_NAME")), name + " is single-column");
            }
        }
        return out;
    }

    private void exerciseSetIfSentinel(BackendV2 backend, String storeName) throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        StoreId sessionStore = StoreId.grim(storeName + "_sessions_" + suffix);
        Entity<UUID, SessionEvent, SessionRecord> sessionsKind = V2BuiltinKinds.sessions();
        KindAdapter<Entity<UUID, SessionEvent, SessionRecord>> sessionAdapter = backend.adapterFor(sessionsKind)
                .orElseThrow(() -> new AssertionError(backend.id() + " has no Session Entity adapter"));
        sessionAdapter.ensureStore(sessionStore, sessionsKind);

        var sessionHandler = sessionAdapter.writeHandler(sessionStore, sessionsKind, Categories.SESSION);
        UUID player = UUID.randomUUID();
        UUID startupS = UUID.randomUUID();
        UUID startupT = UUID.randomUUID();
        UUID firstOpen = UUID.randomUUID();
        UUID secondOpen = UUID.randomUUID();
        UUID alreadyClosed = UUID.randomUUID();
        UUID otherStartup = UUID.randomUUID();
        long now = System.currentTimeMillis();
        long oldClosedAt = now + 5_000L;
        writeSession(sessionHandler, firstOpen, player, startupS, now, SessionRecord.OPEN, 0L);
        writeSession(sessionHandler, secondOpen, player, startupS, now + 1_000L, SessionRecord.OPEN, 1L);
        writeSession(sessionHandler, alreadyClosed, player, startupS, now + 2_000L, oldClosedAt, 2L);
        writeSession(sessionHandler, otherStartup, player, startupT, now + 3_000L, SessionRecord.OPEN, 3L);

        EntityOps.SetIfSentinelOp closeSessions = new EntityOps.SetIfSentinelOp(
                Categories.SESSION, "by_startup_open", startupS, "closed_at", SessionRecord.OPEN, null, "last_activity");
        long changed = sessionAdapter.execute(sessionStore, sessionsKind, closeSessions);
        assertEquals(2L, changed, backend.id() + ": closes every open session for one startup");

        SessionRecord first = readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, firstOpen);
        SessionRecord second = readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, secondOpen);
        SessionRecord closed = readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, alreadyClosed);
        SessionRecord other = readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, otherStartup);
        assertEquals(first.lastActivityEpochMs(), first.closedAtEpochMs(),
                backend.id() + ": first session copies its own last activity");
        assertEquals(second.lastActivityEpochMs(), second.closedAtEpochMs(),
                backend.id() + ": second session copies its own last activity");
        assertEquals(oldClosedAt, closed.closedAtEpochMs(), backend.id() + ": already-closed session keeps its close time");
        assertEquals(SessionRecord.OPEN, other.closedAtEpochMs(), backend.id() + ": other startup remains open");
        assertEquals(0L, (long) sessionAdapter.execute(sessionStore, sessionsKind, closeSessions),
                backend.id() + ": repeated session close changes no rows");

        StoreId startupStore = StoreId.grim(storeName + "_startups_" + suffix);
        Entity<UUID, ServerStartupEvent, ServerStartupRecord> startupsKind = V2BuiltinKinds.serverStartups();
        KindAdapter<Entity<UUID, ServerStartupEvent, ServerStartupRecord>> startupAdapter =
                backend.adapterFor(startupsKind)
                        .orElseThrow(() -> new AssertionError(backend.id() + " has no ServerStartup Entity adapter"));
        startupAdapter.ensureStore(startupStore, startupsKind);
        var startupHandler = startupAdapter.writeHandler(startupStore, startupsKind, Categories.SERVER_STARTUP);
        UUID startupId = UUID.randomUUID();
        long explicitClose = now + 10_000L;
        writeStartup(startupHandler, startupId, UUID.randomUUID(), "test", now, now, ServerStartupRecord.OPEN);

        EntityOps.CountByIndexOp openStartups = new EntityOps.CountByIndexOp(
                Categories.SERVER_STARTUP, "by_open_heartbeat", ServerStartupRecord.OPEN);
        assertEquals(1L, (long) startupAdapter.execute(startupStore, startupsKind, openStartups),
                backend.id() + ": seeded startup is indexed as open");
        EntityOps.SetIfSentinelOp closeStartup = new EntityOps.SetIfSentinelOp(
                Categories.SERVER_STARTUP, null, startupId, "closed_at", ServerStartupRecord.OPEN, explicitClose, null);
        assertEquals(1L, (long) startupAdapter.execute(startupStore, startupsKind, closeStartup),
                backend.id() + ": closes one startup by id");
        ServerStartupRecord startup = readEntity(
                startupAdapter, startupStore, startupsKind, Categories.SERVER_STARTUP, startupId);
        assertEquals(explicitClose, startup.closedAtEpochMs(), backend.id() + ": startup receives the explicit close time");
        assertEquals(0L, (long) startupAdapter.execute(startupStore, startupsKind, openStartups),
                backend.id() + ": closed startup leaves the open index");
        assertEquals(0L, (long) startupAdapter.execute(startupStore, startupsKind, closeStartup),
                backend.id() + ": repeated startup close changes no rows");

        // The remaining branch pair: explicit value by index, and copy-from-field by id.
        UUID startupU = UUID.randomUUID();
        UUID openByIndex = UUID.randomUUID();
        UUID openById = UUID.randomUUID();
        writeSession(sessionHandler, openByIndex, player, startupU, now + 4_000L, SessionRecord.OPEN, 4L);
        writeSession(sessionHandler, openById, player, startupT, now + 5_000L, SessionRecord.OPEN, 5L);
        assertEquals(1L, (long) sessionAdapter.execute(sessionStore, sessionsKind, new EntityOps.SetIfSentinelOp(
                Categories.SESSION, "by_startup_open", startupU, "closed_at", SessionRecord.OPEN, explicitClose, null)));
        assertEquals(explicitClose, readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, openByIndex).closedAtEpochMs(),
                backend.id() + ": explicit value applies by index");
        assertEquals(1L, (long) sessionAdapter.execute(sessionStore, sessionsKind, new EntityOps.SetIfSentinelOp(
                Categories.SESSION, null, openById, "closed_at", SessionRecord.OPEN, null, "last_activity")));
        SessionRecord byId = readEntity(sessionAdapter, sessionStore, sessionsKind, Categories.SESSION, openById);
        assertEquals(byId.lastActivityEpochMs(), byId.closedAtEpochMs(), backend.id() + ": copy from field applies by id");
    }

    private static <ID, E, R> R readEntity(KindAdapter<Entity<ID, E, R>> adapter, StoreId store,
            Entity<ID, E, R> kind, Category<?> category, ID id) throws Exception {
        Optional<R> record = adapter.execute(store, kind, new EntityOps.GetByIdOp<>(category, id));
        return record.orElseThrow(() -> new AssertionError("missing seeded entity " + id));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exerciseEntityIndexes(
            BackendV2 backend,
            String playerStoreName,
            String sessionStoreName,
            String startupStoreName) throws Exception {
        String suffix = Long.toHexString(System.nanoTime());
        StoreId playerStore = StoreId.grim(playerStoreName + "_" + suffix);
        StoreId sessionStore = StoreId.grim(sessionStoreName + "_" + suffix);
        StoreId startupStore = StoreId.grim(startupStoreName + "_" + suffix);

        Entity playersKind = V2BuiltinKinds.players();
        KindAdapter adapter = backend.adapterFor(playersKind).orElseThrow(
            () -> new AssertionError(backend.id() + " has no Entity adapter"));

        adapter.ensureStore(playerStore, playersKind);

        var handler = adapter.writeHandler(playerStore, playersKind, Categories.PLAYER_IDENTITY);
        var event = new ac.grim.grimac.api.storage.event.PlayerIdentityEvent();
        UUID testUuid = UUID.randomUUID();
        String testName = "V2Integ" + testUuid.toString().replace("-", "").substring(0, 12);
        event.uuid(testUuid);
        event.currentName(testName);
        event.firstSeenEpochMs(System.currentTimeMillis());
        event.lastSeenEpochMs(System.currentTimeMillis());
        handler.onEvent(event, 0, true);

        String updatedName = testName + "Z";
        long nowForIdentity = System.currentTimeMillis();
        adapter.execute(playerStore, playersKind, new EntityOps.UpsertOp<>(
                Categories.PLAYER_IDENTITY,
                new PlayerIdentity(testUuid, updatedName, nowForIdentity - 1000L, nowForIdentity + 1000L)));

        EntityOps.GetByIdOp getOp = new EntityOps.GetByIdOp(Categories.PLAYER_IDENTITY, testUuid);
        Optional<PlayerIdentity> result = (Optional<PlayerIdentity>) adapter.execute(playerStore, playersKind, getOp);

        assertTrue(result.isPresent(), backend.id() + ": should read back written player");
        assertEquals(updatedName, result.get().currentName(), backend.id() + ": name match after UpsertOp");
        assertEquals(testUuid, result.get().uuid(), backend.id() + ": uuid match");

        EntityOps.FindByIndexOp<PlayerIdentity> byNameOp = new EntityOps.FindByIndexOp<>(
            Categories.PLAYER_IDENTITY, "by_name", updatedName.toLowerCase(java.util.Locale.ROOT), null, 10);
        var byName = (ac.grim.grimac.api.storage.query.Page<PlayerIdentity>)
            adapter.execute(playerStore, playersKind, byNameOp);
        assertTrue(byName.items().stream().anyMatch(p -> p.uuid().equals(testUuid)),
            backend.id() + ": case-insensitive player exact lookup");

        EntityOps.PrefixIndexOp<PlayerIdentity> byPrefixOp = new EntityOps.PrefixIndexOp<>(
            Categories.PLAYER_IDENTITY, "by_name", updatedName.substring(0, 7).toUpperCase(java.util.Locale.ROOT), null, 10);
        var byPrefix = (ac.grim.grimac.api.storage.query.Page<PlayerIdentity>)
            adapter.execute(playerStore, playersKind, byPrefixOp);
        assertTrue(byPrefix.items().stream().anyMatch(p -> p.uuid().equals(testUuid)),
            backend.id() + ": case-insensitive player prefix lookup");

        Entity sessionsKind = V2BuiltinKinds.sessions();
        KindAdapter sessionAdapter = backend.adapterFor(sessionsKind).orElseThrow(
            () -> new AssertionError(backend.id() + " has no Session Entity adapter"));
        sessionAdapter.ensureStore(sessionStore, sessionsKind);
        var sessionHandler = sessionAdapter.writeHandler(sessionStore, sessionsKind, Categories.SESSION);
        UUID firstSession = UUID.randomUUID();
        UUID secondSession = UUID.randomUUID();
        UUID closedSession = UUID.randomUUID();
        UUID otherStartupSession = UUID.randomUUID();
        UUID startup = UUID.randomUUID();
        UUID otherStartup = UUID.randomUUID();
        long now = System.currentTimeMillis();
        writeSession(sessionHandler, firstSession, testUuid, startup, now - 1000L, 0);
        writeSession(sessionHandler, secondSession, testUuid, startup, now, 1);
        writeSession(sessionHandler, closedSession, testUuid, startup, now + 1000L, now + 2000L, 2);
        writeSession(sessionHandler, otherStartupSession, testUuid, otherStartup, now + 2000L, SessionRecord.OPEN, 3);

        EntityOps.FindByIndexOp<SessionRecord> byPlayerOp = new EntityOps.FindByIndexOp<>(
            Categories.SESSION, "by_player_started", testUuid, null, 10);
        var byPlayer = (ac.grim.grimac.api.storage.query.Page<SessionRecord>)
            sessionAdapter.execute(sessionStore, sessionsKind, byPlayerOp);
        assertEquals(4, byPlayer.items().size(), backend.id() + ": session index row count");
        assertEquals(otherStartupSession, byPlayer.items().get(0).sessionId(),
            backend.id() + ": by_player_started returns newest session first");
        assertEquals(closedSession, byPlayer.items().get(1).sessionId(),
            backend.id() + ": by_player_started returns closed session in started order");
        assertEquals(secondSession, byPlayer.items().get(2).sessionId(),
            backend.id() + ": by_player_started returns middle session third");
        assertEquals(firstSession, byPlayer.items().get(3).sessionId(),
            backend.id() + ": by_player_started returns oldest session fourth");

        EntityOps.CountByIndexOp countOp = new EntityOps.CountByIndexOp(
            Categories.SESSION, "by_player_started", testUuid);
        long count = (Long) sessionAdapter.execute(sessionStore, sessionsKind, countOp);
        assertEquals(4L, count, backend.id() + ": session count by player");

        EntityOps.FindByIndexOp<SessionRecord> byStartupOpenOp = new EntityOps.FindByIndexOp<>(
            Categories.SESSION, "by_startup_open", startup, null, 10);
        var byStartupOpen = (ac.grim.grimac.api.storage.query.Page<SessionRecord>)
            sessionAdapter.execute(sessionStore, sessionsKind, byStartupOpenOp);
        assertEquals(3, byStartupOpen.items().size(), backend.id() + ": by_startup_open filters startup id");
        assertFalse(byStartupOpen.items().stream().anyMatch(s -> s.sessionId().equals(otherStartupSession)),
            backend.id() + ": by_startup_open excludes other startup rows");
        int firstClosed = -1;
        for (int i = 0; i < byStartupOpen.items().size(); i++) {
            if (byStartupOpen.items().get(i).isClosed()) {
                firstClosed = i;
                break;
            }
        }
        assertEquals(2, firstClosed, backend.id() + ": by_startup_open orders open sessions before closed rows");
        assertTrue(byStartupOpen.items().subList(0, firstClosed).stream().noneMatch(SessionRecord::isClosed),
            backend.id() + ": by_startup_open leading rows are open");

        Entity startupsKind = V2BuiltinKinds.serverStartups();
        KindAdapter startupAdapter = backend.adapterFor(startupsKind).orElseThrow(
            () -> new AssertionError(backend.id() + " has no ServerStartup Entity adapter"));
        startupAdapter.ensureStore(startupStore, startupsKind);
        var startupHandler = startupAdapter.writeHandler(startupStore, startupsKind, Categories.SERVER_STARTUP);
        UUID instance = UUID.randomUUID();
        UUID firstStartup = UUID.randomUUID();
        UUID secondStartup = UUID.randomUUID();
        UUID closedStartup = UUID.randomUUID();
        byte[] emptyManifest = new byte[] {1, 1, 0, 0};
        byte[] grownManifest = new byte[] {1, 1, 0, 1, 42, 7};
        writeStartup(startupHandler, firstStartup, instance, "test", now - 3000L, now - 2000L,
                ServerStartupRecord.OPEN, emptyManifest);
        writeStartup(startupHandler, firstStartup, instance, "test", now - 3000L, now - 1500L,
                ServerStartupRecord.OPEN, grownManifest);
        writeStartup(startupHandler, secondStartup, instance, "test", now - 2000L, now - 1000L, ServerStartupRecord.OPEN);
        writeStartup(startupHandler, closedStartup, instance, "test", now - 1000L, now, now + 1000L);

        EntityOps.GetByIdOp<UUID, ServerStartupRecord> firstStartupById = new EntityOps.GetByIdOp<>(
                Categories.SERVER_STARTUP, firstStartup);
        Optional<ServerStartupRecord> firstStartupRecord = (Optional<ServerStartupRecord>)
                startupAdapter.execute(startupStore, startupsKind, firstStartupById);
        assertTrue(firstStartupRecord.isPresent(), backend.id() + ": startup row reloads by id");
        assertArrayEquals(grownManifest, firstStartupRecord.get().verboseManifest(),
                backend.id() + ": startup verbose manifest grows after template registration");

        EntityOps.FindByIndexOp<ServerStartupRecord> byInstanceOpenOp = new EntityOps.FindByIndexOp<>(
            Categories.SERVER_STARTUP, "by_instance_open", instance, null, 10);
        var byInstanceOpen = (ac.grim.grimac.api.storage.query.Page<ServerStartupRecord>)
            startupAdapter.execute(startupStore, startupsKind, byInstanceOpenOp);
        assertEquals(3, byInstanceOpen.items().size(), backend.id() + ": by_instance_open filters instance id");
        assertFalse(byInstanceOpen.items().get(0).isClosed(), backend.id() + ": startup open index starts with open rows");

        EntityOps.FindByIndexOp<ServerStartupRecord> openByHeartbeatOp = new EntityOps.FindByIndexOp<>(
            Categories.SERVER_STARTUP, "by_open_heartbeat", ServerStartupRecord.OPEN, null, 10);
        var openByHeartbeat = (ac.grim.grimac.api.storage.query.Page<ServerStartupRecord>)
            startupAdapter.execute(startupStore, startupsKind, openByHeartbeatOp);
        assertTrue(openByHeartbeat.items().stream().anyMatch(s -> s.startupId().equals(firstStartup)),
            backend.id() + ": by_open_heartbeat includes open startup");
        assertFalse(openByHeartbeat.items().stream().anyMatch(s -> s.startupId().equals(closedStartup)),
            backend.id() + ": by_open_heartbeat excludes closed startup");

        LOG.info(() -> backend.id() + ": v2 entity index contract OK for " + testUuid);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exerciseSettingsKv(BackendV2 backend, String settingsStoreName) throws Exception {
        StoreId settingsStore = StoreId.grim(settingsStoreName + "_" + Long.toHexString(System.nanoTime()));
        KeyValueScoped settingsKind = V2BuiltinKinds.settings();
        KindAdapter adapter = backend.adapterFor(settingsKind).orElseThrow(
            () -> new AssertionError(backend.id() + " has no KeyValueScoped adapter"));

        adapter.ensureStore(settingsStore, settingsKind);

        var handler = adapter.writeHandler(settingsStore, settingsKind, Categories.SETTING);
        SettingEvent event = new SettingEvent();
        event.scope(SettingScope.PLAYER)
            .scopeKey("00000000-0000-0000-0000-000000000001")
            .key("alerts")
            .value(new byte[]{1})
            .updatedEpochMs(System.currentTimeMillis());
        handler.onEvent(event, 0, true);

        KeyValueScopedOps.GetOp<SettingScope, byte[]> get = new KeyValueScopedOps.GetOp<>(
            Categories.SETTING, SettingScope.PLAYER,
            "00000000-0000-0000-0000-000000000001", "alerts");
        Optional<byte[]> got = (Optional<byte[]>) adapter.execute(settingsStore, settingsKind, get);
        assertTrue(got.isPresent(), backend.id() + ": setting KV row round-trips");
        assertArrayEquals(new byte[]{1}, got.get(), backend.id() + ": setting KV value round-trips");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void exerciseCounter(BackendV2 backend, String counterStoreName) throws Exception {
        StoreId counterStore = StoreId.grim(counterStoreName + "_" + Long.toHexString(System.nanoTime()));
        Counter<String> counterKind = Counter.<String>builder()
            .name("test-counter")
            .key(String.class, STRING_CODEC)
            .build();
        KindAdapter adapter = backend.adapterFor(counterKind).orElseThrow(
            () -> new AssertionError(backend.id() + " has no Counter adapter"));

        adapter.ensureStore(counterStore, counterKind);

        var handler = adapter.writeHandler(counterStore, counterKind, (ac.grim.grimac.api.storage.category.Category) Categories.SETTING);
        CounterEvent<String> event = new CounterEvent<>();
        event.key = "flags";
        event.delta = 2L;
        handler.onEvent(event, 0, true);

        long afterIncrement = (Long) adapter.execute(counterStore, counterKind,
            new CounterOps.IncrementByOp<>(Categories.SETTING, "flags", 3L));
        assertEquals(5L, afterIncrement, backend.id() + ": counter increment merges");

        long afterLowerSet = (Long) adapter.execute(counterStore, counterKind,
            new CounterOps.SetIfHigherOp<>(Categories.SETTING, "flags", 4L));
        assertEquals(5L, afterLowerSet, backend.id() + ": counter setIfHigher preserves higher value");

        long afterHigherSet = (Long) adapter.execute(counterStore, counterKind,
            new CounterOps.SetIfHigherOp<>(Categories.SETTING, "flags", 9L));
        assertEquals(9L, afterHigherSet, backend.id() + ": counter setIfHigher raises value");
    }

    private void exerciseOwnership(BackendV2 backend, String ownershipStoreName) throws Exception {
        StoreId ownershipStore = StoreId.grim(ownershipStoreName + "_" + Long.toHexString(System.nanoTime()));
        ServerOwnershipAdapter ownership = backend.ownershipAdapter().orElseThrow(
            () -> new AssertionError(backend.id() + " has no ownership adapter"));
        ownership.ensureStore(ownershipStore);

        UUID persistentId = UUID.randomUUID();
        UUID firstStartup = UUID.randomUUID();
        UUID firstFence = UUID.randomUUID();
        UUID secondStartup = UUID.randomUUID();
        UUID secondFence = UUID.randomUUID();
        ServerOwnershipMetadata metadata = new ServerOwnershipMetadata(
            "server-a", "localhost", "test", "1.21.11");

        OwnershipClaimResult firstClaim = ownership.claimOwnership(
            ownershipStore, persistentId, firstStartup, firstFence, 250L, metadata);
        assertTrue(firstClaim.claimed(), backend.id() + ": first ownership claim succeeds");
        assertTrue(ownership.readOwnership(ownershipStore, persistentId).isPresent(),
            backend.id() + ": claimed ownership row is readable");

        OwnershipClaimResult duplicateClaim = ownership.claimOwnership(
            ownershipStore, persistentId, secondStartup, secondFence, 250L, metadata);
        assertFalse(duplicateClaim.claimed(), backend.id() + ": active duplicate claim is denied");
        assertEquals(firstStartup, duplicateClaim.currentOwner().ownerStartupId(),
            backend.id() + ": duplicate denial reports current owner");

        OwnershipRenewResult renewed = ownership.renewOwnership(
            ownershipStore, persistentId, firstStartup, firstFence, 250L);
        assertTrue(renewed.renewed(), backend.id() + ": current owner renews");

        Thread.sleep(300L);

        OwnershipClaimResult takeover = ownership.claimOwnership(
            ownershipStore, persistentId, secondStartup, secondFence, 1_000L, metadata);
        assertTrue(takeover.claimed(), backend.id() + ": expired ownership can be claimed");
        assertEquals(firstStartup, takeover.previousOwner().ownerStartupId(),
            backend.id() + ": takeover reports expired owner for recovery");

        OwnershipRenewResult staleRenew = ownership.renewOwnership(
            ownershipStore, persistentId, firstStartup, firstFence, 1_000L);
        assertFalse(staleRenew.renewed(), backend.id() + ": old fence cannot renew after takeover");

        assertFalse(ownership.closeOwnership(ownershipStore, persistentId, firstStartup, firstFence, "stale"),
            backend.id() + ": old fence cannot close new owner");
        assertTrue(ownership.closeOwnership(ownershipStore, persistentId, secondStartup, secondFence, "shutdown"),
            backend.id() + ": current owner closes cleanly");

        OwnershipClaimResult afterClose = ownership.claimOwnership(
            ownershipStore, persistentId, UUID.randomUUID(), UUID.randomUUID(), 1_000L, metadata);
        assertTrue(afterClose.claimed(), backend.id() + ": closed ownership can be claimed immediately");
    }

    private static void writeSession(ac.grim.grimac.api.storage.backend.StorageEventHandler<SessionEvent> handler,
                                     UUID sessionId, UUID playerId, UUID startupId, long started, long sequence) throws Exception {
        writeSession(handler, sessionId, playerId, startupId, started, SessionRecord.OPEN, sequence);
    }

    private static void writeSession(ac.grim.grimac.api.storage.backend.StorageEventHandler<SessionEvent> handler,
                                     UUID sessionId, UUID playerId, UUID startupId, long started,
                                     long closedAtEpochMs, long sequence) throws Exception {
        SessionEvent se = new SessionEvent();
        se.sessionId(sessionId)
            .playerUuid(playerId)
            .startupId(startupId)
            .startedEpochMs(started)
            .lastActivityEpochMs(started + 100L)
            .closedAtEpochMs(closedAtEpochMs)
            .grimVersion("test")
            .clientBrand("vanilla")
            .clientVersion(772)
            .serverVersionString("1.21.11");
        handler.onEvent(se, sequence, true);
    }

    private static void writeStartup(ac.grim.grimac.api.storage.backend.StorageEventHandler<ServerStartupEvent> handler,
                                     UUID startupId, UUID instanceId, String serverName, long started,
                                     long lastHeartbeat, long closedAt) throws Exception {
        writeStartup(handler, startupId, instanceId, serverName, started, lastHeartbeat, closedAt, null);
    }

    private static void writeStartup(ac.grim.grimac.api.storage.backend.StorageEventHandler<ServerStartupEvent> handler,
                                     UUID startupId, UUID instanceId, String serverName, long started,
                                     long lastHeartbeat, long closedAt, byte[] verboseManifest) throws Exception {
        ServerStartupEvent se = new ServerStartupEvent();
        se.startupId(startupId)
            .instanceId(instanceId)
            .serverName(serverName)
            .startedEpochMs(started)
            .lastHeartbeatEpochMs(lastHeartbeat)
            .closedAtEpochMs(closedAt)
            .grimVersion("test")
            .serverVersionString("1.21.11")
            .hostname("localhost")
            .closeReason(closedAt == ServerStartupRecord.OPEN ? null : "graceful")
            .verboseManifest(verboseManifest);
        handler.onEvent(se, 0L, true);
    }

    private static BackendContext ctx(BackendConfig cfg) {
        return ctx(cfg, Path.of("/tmp"));
    }

    private static BackendContext ctx(BackendConfig cfg, Path dataDir) {
        return new BackendContext() {
            @Override public Logger logger() { return LOG; }
            @Override public Path dataDirectory() { return dataDir; }
            @Override public BackendConfig config() { return cfg; }
        };
    }

    private static final Codec<String> STRING_CODEC = new Codec<>() {
        @Override public Class<String> recordType() { return String.class; }
        @Override public EncodeShape shape() { throw new UnsupportedOperationException("counter key only"); }
        @Override public int version() { return 1; }
        @Override public Object indexField(String record, String fieldName) { return null; }
    };

    private static void assumeReachable(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            Assumptions.assumeTrue(s.isConnected());
        } catch (Exception e) {
            Assumptions.assumeTrue(false, host + ":" + port + " unreachable");
        }
    }
}
