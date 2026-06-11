package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.codec.Codec;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.SettingEvent;
import ac.grim.grimac.api.storage.event.ServerStartupEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.CounterEvent;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.ops.CounterOps;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoBackendV2;
import ac.grim.grimac.internal.storage.backend.postgres.PostgresBackendConfig;
import ac.grim.grimac.internal.storage.backend.postgres.v2.PostgresBackendV2;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackendConfig;
import ac.grim.grimac.internal.storage.backend.redis.v2.RedisBackendV2;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.backend.sqlite.v2.SqliteBackendV2;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
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
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Postgres v2: entity write + read")
    void postgres() throws Exception {
        assumeReachable("localhost", 5432);
        PostgresBackendConfig cfg = new PostgresBackendConfig(
            "localhost", 5432, "grim", "postgres", "grim-test-postgres", "", 256, TableNames.DEFAULTS);
        PostgresBackendV2 backend = new PostgresBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_pg", "v2_integ_sessions_pg",
                    "v2_integ_startups_pg");
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Redis v2: entity indexes")
    void redis() throws Exception {
        assumeReachable("localhost", 6379);
        RedisBackendConfig cfg = new RedisBackendConfig(
            "localhost", 6379, 0, null, "grim-test-redis", "v2integ:", 2000, 256, false, TableNames.DEFAULTS);
        RedisBackendV2 backend = new RedisBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_redis", "v2_integ_sessions_redis",
                    "v2_integ_startups_redis");
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("MySQL v2: entity indexes")
    void mysql() throws Exception {
        assumeReachable("localhost", 3306);
        ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig cfg =
            new ac.grim.grimac.internal.storage.backend.mysql.MysqlBackendConfig(
                "localhost", 3306, "grim", "grim", "grim-test-mysql", "", 256, TableNames.DEFAULTS);
        ac.grim.grimac.internal.storage.backend.mysql.v2.MysqlBackendV2 backend =
            new ac.grim.grimac.internal.storage.backend.mysql.v2.MysqlBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_mysql", "v2_integ_sessions_mysql",
                    "v2_integ_startups_mysql");
        } finally {
            backend.close();
        }
    }

    @Test @DisplayName("Mongo v2: entity indexes")
    void mongo() throws Exception {
        assumeReachable("localhost", 27017);
        MongoBackendConfig cfg = new MongoBackendConfig(
            "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin",
            "v2_integration_test", 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        try {
            backend.init(ctx(cfg));
            exerciseEntityIndexes(backend, "v2_integ_players_mongo", "v2_integ_sessions_mongo",
                    "v2_integ_startups_mongo");
        } finally {
            backend.close();
        }
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

        EntityOps.GetByIdOp getOp = new EntityOps.GetByIdOp(Categories.PLAYER_IDENTITY, testUuid);
        Optional<PlayerIdentity> result = (Optional<PlayerIdentity>) adapter.execute(playerStore, playersKind, getOp);

        assertTrue(result.isPresent(), backend.id() + ": should read back written player");
        assertEquals(testName, result.get().currentName(), backend.id() + ": name match");
        assertEquals(testUuid, result.get().uuid(), backend.id() + ": uuid match");

        EntityOps.FindByIndexOp<PlayerIdentity> byNameOp = new EntityOps.FindByIndexOp<>(
            Categories.PLAYER_IDENTITY, "by_name", testName.toLowerCase(java.util.Locale.ROOT), null, 10);
        var byName = (ac.grim.grimac.api.storage.query.Page<PlayerIdentity>)
            adapter.execute(playerStore, playersKind, byNameOp);
        assertTrue(byName.items().stream().anyMatch(p -> p.uuid().equals(testUuid)),
            backend.id() + ": case-insensitive player exact lookup");

        EntityOps.PrefixIndexOp<PlayerIdentity> byPrefixOp = new EntityOps.PrefixIndexOp<>(
            Categories.PLAYER_IDENTITY, "by_name", testName.substring(0, 7).toUpperCase(java.util.Locale.ROOT), null, 10);
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
                backend.id() + ": startup verbose manifest grows after lazy template registration");

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
