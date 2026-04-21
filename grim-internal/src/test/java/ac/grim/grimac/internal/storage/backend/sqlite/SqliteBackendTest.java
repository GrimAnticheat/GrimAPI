package ac.grim.grimac.internal.storage.backend.sqlite;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SqliteBackendTest {

    private static final Logger LOG = Logger.getLogger("SqliteBackendTest");

    @TempDir Path tmp;
    private SqliteBackend backend;

    @BeforeEach
    void setup() throws Exception {
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("history.v1.db");
        backend = new SqliteBackend(cfg);
        backend.init(new TestContext(cfg, LOG, tmp));
    }

    @AfterEach
    void teardown() throws Exception {
        backend.close();
    }

    @Test
    void schemaVersionIsOneAfterInit() throws Exception {
        try (var c = java.sql.DriverManager.getConnection(backend.jdbcUrl())) {
            assertEquals(SqliteSchema.CURRENT_VERSION, SqliteSchema.readSchemaVersion(c));
        }
    }

    @Test
    void writeAndReadSessionPage() throws Exception {
        UUID player = UUID.randomUUID();
        List<SessionRecord> sessions = new ArrayList<>();
        for (int i = 0; i < 5; i++) sessions.add(session(player, (i + 1) * 1000L));
        backend.writeRecordsDirect(Categories.SESSION, sessions);

        Page<SessionRecord> page = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(player, 3, null));
        assertEquals(3, page.items().size());
        assertEquals(5000, page.items().get(0).startedEpochMs(), "newest first");
        assertNotNull(page.nextCursor());

        Cursor next = page.nextCursor();
        Page<SessionRecord> page2 = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(player, 3, next));
        assertEquals(2, page2.items().size());
        assertNull(page2.nextCursor());
    }

    @Test
    void writeAndCountViolations() throws Exception {
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        List<ViolationRecord> v = new ArrayList<>();
        for (int i = 0; i < 25; i++) v.add(violation(session, player, 1000L + i));
        backend.writeRecordsDirect(Categories.VIOLATION, v);
        assertEquals(25, backend.countViolationsInSession(session));
        Page<ViolationRecord> p = backend.read(Categories.VIOLATION, Queries.listViolationsInSession(session, 10, null));
        assertEquals(10, p.items().size());
        assertEquals(1000L, p.items().get(0).occurredEpochMs());
    }

    @Test
    void upsertSession() throws Exception {
        UUID player = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        SessionRecord v1 = new SessionRecord(sid, player, "Prison", 1000, 1000, "3.1.0", "vanilla", 767, "Paper", List.of());
        SessionRecord v2 = new SessionRecord(sid, player, "Prison", 1000, 5000, "3.1.0", "vanilla", 767, "Paper", List.of());
        backend.writeRecordsDirect(Categories.SESSION, List.of(v1));
        backend.writeRecordsDirect(Categories.SESSION, List.of(v2));
        Page<SessionRecord> p = backend.read(Categories.SESSION, Queries.getSessionById(sid));
        assertEquals(1, p.items().size());
        assertEquals(5000, p.items().get(0).lastActivityEpochMs());
    }

    @Test
    void identityUpsertMergesFirstAndLastSeen() throws Exception {
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(new PlayerIdentity(player, "OldName", 2000, 3000)));
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(new PlayerIdentity(player, "NewName", 1000, 5000)));
        Page<PlayerIdentity> p = backend.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(player));
        assertEquals(1, p.items().size());
        PlayerIdentity id = p.items().get(0);
        assertEquals("NewName", id.currentName());
        assertEquals(1000, id.firstSeenEpochMs(), "min firstSeen");
        assertEquals(5000, id.lastSeenEpochMs(), "max lastSeen");
    }

    @Test
    void nameLookupCaseInsensitive() throws Exception {
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(new PlayerIdentity(player, "Alice", 1000, 1000)));
        Page<PlayerIdentity> p = backend.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentityByName("ALICE"));
        assertEquals(1, p.items().size());
        assertEquals(player, p.items().get(0).uuid());
    }

    @Test
    void settingRoundTrip() throws Exception {
        String key = "alerts";
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.SETTING, List.of(
                new SettingRecord(SettingScope.PLAYER, player.toString(), key, "true".getBytes(), 1000)));
        Page<SettingRecord> p = backend.read(Categories.SETTING,
                Queries.getSetting(SettingScope.PLAYER, player.toString(), key));
        assertEquals(1, p.items().size());
        assertEquals("true", p.items().get(0).asString());
    }

    @Test
    void deleteByPlayerCascadesSessionsAndViolations() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID aliceSession = UUID.randomUUID();
        UUID bobSession = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.SESSION, List.of(
                new SessionRecord(aliceSession, alice, "Prison", 1000, 1000, "3.1.0", "vanilla", 767, "Paper", List.of()),
                new SessionRecord(bobSession, bob, "Prison", 2000, 2000, "3.1.0", "vanilla", 767, "Paper", List.of())));
        backend.writeRecordsDirect(Categories.VIOLATION, List.of(
                violation(aliceSession, alice, 1100),
                violation(bobSession, bob, 2100)));

        backend.delete(Categories.SESSION, Deletes.byPlayer(alice));
        Page<SessionRecord> aliceP = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(alice, 10, null));
        assertTrue(aliceP.items().isEmpty());
        assertEquals(0, backend.countViolationsInSession(aliceSession));

        Page<SessionRecord> bobP = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(bob, 10, null));
        assertEquals(1, bobP.items().size(), "bob's session intact");
        assertEquals(1, backend.countViolationsInSession(bobSession));
    }

    @Test
    void pagedReadPerformance100kViolations() throws Exception {
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.SESSION, List.of(
                new SessionRecord(session, player, "Prison", 1000, 1000, "3.1.0", "vanilla", 767, "Paper", List.of())));

        List<ViolationRecord> batch = new ArrayList<>(1000);
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            batch.add(violation(session, player, 1000L + i));
            if (batch.size() == 1000) {
                backend.writeRecordsDirect(Categories.VIOLATION, batch);
                batch = new ArrayList<>(1000);
            }
        }
        if (!batch.isEmpty()) backend.writeRecordsDirect(Categories.VIOLATION, batch);

        // First page of 15 should come back fast.
        long start = System.nanoTime();
        Page<ViolationRecord> p = backend.read(Categories.VIOLATION, Queries.listViolationsInSession(session, 15, null));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(15, p.items().size());
        // Exit-criteria expects <100ms for paged read on a local test corpus of 100k.
        assertTrue(elapsedMs < 100, "paged read took " + elapsedMs + "ms for 100k corpus (expected <100ms)");
    }

    private static SessionRecord session(UUID player, long started) {
        return new SessionRecord(UUID.randomUUID(), player, "Prison", started, started,
                "3.1.0", "vanilla", 767, "Paper", List.of());
    }

    private static ViolationRecord violation(UUID sessionId, UUID player, long time) {
        return new ViolationRecord(0, sessionId, player, 1, 1.0, time, "v", VerboseFormat.TEXT);
    }

    private record TestContext(BackendConfig config, Logger logger, Path dataDirectory) implements BackendContext {}
}
