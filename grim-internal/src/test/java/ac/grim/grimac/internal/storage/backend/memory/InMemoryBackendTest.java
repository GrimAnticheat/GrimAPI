package ac.grim.grimac.internal.storage.backend.memory;

import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the backend's read/delete surface + the {@code writeRecordsDirect}
 * bulk-load path used by the migrator. The ring-fed event path has integration
 * coverage via the DataStoreImpl tests.
 */
final class InMemoryBackendTest {

    private InMemoryBackend backend;

    @BeforeEach
    void setup() {
        backend = new InMemoryBackend();
    }

    @Test
    void writeAndReadSessionsByPlayerNewestFirst() throws Exception {
        UUID player = UUID.randomUUID();
        SessionRecord s1 = session(player, 1000);
        SessionRecord s2 = session(player, 2000);
        SessionRecord s3 = session(player, 3000);
        backend.writeRecordsDirect(Categories.SESSION, List.of(s1, s2, s3));

        Page<SessionRecord> page = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(player, 2, null));
        assertEquals(2, page.items().size());
        assertEquals(3000, page.items().get(0).startedEpochMs());
        assertEquals(2000, page.items().get(1).startedEpochMs());
        assertNotNull(page.nextCursor());

        Page<SessionRecord> page2 = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(player, 2, page.nextCursor()));
        assertEquals(1, page2.items().size());
        assertEquals(1000, page2.items().get(0).startedEpochMs());
        assertNull(page2.nextCursor());
    }

    @Test
    void countViolationsInSession() throws Exception {
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        List<ViolationRecord> bunch = new ArrayList<>();
        for (int i = 0; i < 17; i++) bunch.add(violation(session, player, i * 100L));
        backend.writeRecordsDirect(Categories.VIOLATION, bunch);
        assertEquals(17, backend.countViolationsInSession(session));
    }

    @Test
    void listViolationsInSessionOrderedAscendingTime() throws Exception {
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.VIOLATION, List.of(
                violation(session, player, 3000),
                violation(session, player, 1000),
                violation(session, player, 2000)));
        Page<ViolationRecord> page = backend.read(Categories.VIOLATION, Queries.listViolationsInSession(session, 10, null));
        assertEquals(3, page.items().size());
        assertEquals(1000, page.items().get(0).occurredEpochMs());
        assertEquals(3000, page.items().get(2).occurredEpochMs());
    }

    @Test
    void forgetPlayerWipesAllCategories() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        SessionRecord sA = session(alice, 1000);
        SessionRecord sB = session(bob, 2000);
        backend.writeRecordsDirect(Categories.SESSION, List.of(sA, sB));
        backend.writeRecordsDirect(Categories.VIOLATION, List.of(
                violation(sA.sessionId(), alice, 1100),
                violation(sB.sessionId(), bob, 2100)));
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(
                new PlayerIdentity(alice, "Alice", 0, 1000),
                new PlayerIdentity(bob, "Bob", 0, 2000)));
        backend.writeRecordsDirect(Categories.SETTING, List.of(
                new SettingRecord(SettingScope.PLAYER, alice.toString(), "alerts", "true".getBytes(), 1000)));

        backend.delete(Categories.VIOLATION, Deletes.byPlayer(alice));
        backend.delete(Categories.SESSION, Deletes.byPlayer(alice));
        backend.delete(Categories.PLAYER_IDENTITY, Deletes.byPlayer(alice));
        backend.delete(Categories.SETTING, Deletes.byPlayer(alice));

        Page<SessionRecord> aliceSessions = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(alice, 10, null));
        assertTrue(aliceSessions.items().isEmpty());
        assertEquals(0, backend.countViolationsInSession(sA.sessionId()));
        Page<PlayerIdentity> aliceId = backend.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(alice));
        assertTrue(aliceId.items().isEmpty());

        // Bob's data intact
        Page<SessionRecord> bobSessions = backend.read(Categories.SESSION, Queries.listSessionsByPlayer(bob, 10, null));
        assertEquals(1, bobSessions.items().size());
    }

    @Test
    void playerIdentityMergesOnRepeatedWrites() throws Exception {
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(
                new PlayerIdentity(player, "OldName", 1000, 2000)));
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(
                new PlayerIdentity(player, "NewName", 3000, 4000)));
        Page<PlayerIdentity> p = backend.read(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(player));
        assertEquals(1, p.items().size());
        PlayerIdentity id = p.items().get(0);
        assertEquals("NewName", id.currentName());
        assertEquals(1000, id.firstSeenEpochMs(), "firstSeen preserved across upserts");
        assertEquals(4000, id.lastSeenEpochMs());
    }

    private static SessionRecord session(UUID player, long started) {
        return new SessionRecord(
                UUID.randomUUID(), player, "Prison", started, started,
                "3.1.0", "vanilla", 767, "Paper/1.21.1", List.of());
    }

    private static ViolationRecord violation(UUID sessionId, UUID player, long time) {
        return new ViolationRecord(0, sessionId, player, 1, 1.0, time, "v", VerboseFormat.TEXT);
    }
}
