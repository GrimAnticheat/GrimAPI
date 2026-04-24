package ac.grim.grimac.internal.storage.backend.sqlite;

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
import ac.grim.grimac.internal.storage.backend.sqlite.writers.UpserterFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Parameterised round-trip for {@link SqliteBackend} under both upsert
 * dialects. The legacy dialect only kicks in naturally on pre-3.24 SQLite
 * engines (CraftBukkit 1.8–1.12 era bundled sqlite-jdbc); this test forces
 * it via {@link SqliteBackend#overrideUpserterFactoryForTest} so the two-step
 * {@code INSERT OR IGNORE} + {@code UPDATE} path gets covered by CI against
 * whatever modern engine the test JVM ships.
 * <p>
 * Both paths MUST observe identical read-back semantics: same session,
 * identity, and settings rows after repeated upserts, same first_seen/
 * last_seen merge behaviour on identity, same latest-wins on session
 * fields.
 */
class SqliteBackendDialectTest {

    private enum Dialect {
        MODERN(UpserterFactory.MODERN),
        LEGACY(UpserterFactory.LEGACY);

        final UpserterFactory factory;
        Dialect(UpserterFactory f) { this.factory = f; }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Dialect.class)
    @DisplayName("SQLite upsert dialect — write/read/upsert-merge round-trip")
    void roundTrip(Dialect dialect, @TempDir Path tempDir) throws Exception {
        SqliteBackend b = new SqliteBackend(SqliteBackendConfig.defaults(
                tempDir.resolve("dialect-" + dialect + ".db").toString()));
        b.init(ctx(tempDir));
        b.overrideUpserterFactoryForTest(dialect.factory);

        try {
            UUID player = UUID.randomUUID();
            UUID session = UUID.randomUUID();
            long t0 = 1_700_000_000_000L;

            // --- identity: two writes with bracketing timestamps ---
            // Upsert semantics require first_seen=min, last_seen=max after both
            // writes, and current_name overwritten to the second value.
            StorageEventHandler<PlayerIdentityEvent> ih = b.eventHandlerFor(Categories.PLAYER_IDENTITY);
            ih.onEvent(newIdentity(player, "AlphaBravo", t0 + 100, t0 + 100), 0, false);
            ih.onEvent(newIdentity(player, "charlieDelta", t0, t0 + 500), 1, true);

            // --- session: two writes to exercise UPSERT's DO UPDATE branch ---
            StorageEventHandler<SessionEvent> sh = b.eventHandlerFor(Categories.SESSION);
            sh.onEvent(newSession(session, player, "srv-1", t0, t0), 0, false);
            sh.onEvent(newSession(session, player, "srv-2", t0, t0 + 1000), 1, true);

            // --- violations: 5 rows (no upsert, plain insert) ---
            StorageEventHandler<ViolationEvent> vh = b.eventHandlerFor(Categories.VIOLATION);
            for (int i = 0; i < 5; i++) {
                ViolationEvent v = new ViolationEvent();
                v.sessionId(session).playerUuid(player).checkId(42 + i).vl(1.0 + i)
                        .occurredEpochMs(t0 + i).verbose("v" + i).verboseFormat(VerboseFormat.TEXT);
                vh.onEvent(v, i, i == 4);
            }

            // --- settings: two writes to exercise UPSERT on composite PK ---
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

            Page<PlayerIdentity> byName = b.read(Categories.PLAYER_IDENTITY,
                    new Queries.GetPlayerIdentityByName("charliedelta"));
            assertEquals(1, byName.items().size(), "case-insensitive name lookup");

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
        Logger log = Logger.getLogger("sqlite-dialect-test");
        return new BackendContext() {
            @Override public BackendConfig config() { return null; }
            @Override public Logger logger() { return log; }
            @Override public Path dataDirectory() { return dataDir; }
        };
    }
}
