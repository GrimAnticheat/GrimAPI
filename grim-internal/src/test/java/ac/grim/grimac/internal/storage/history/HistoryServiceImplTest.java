package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.history.CheckBucket;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.history.ViolationEntry;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HistoryServiceImplTest {

    private static final Logger LOG = Logger.getLogger("HistoryServiceImplTest");

    private InMemoryBackend backend;
    private DataStoreImpl store;
    private HistoryServiceImpl service;
    private CheckRegistry registry;
    private int reachId, timerId;

    @BeforeEach
    void setup() {
        backend = new InMemoryBackend();
        Map<Category<?>, Backend> routing = Map.of(
                Categories.VIOLATION, backend,
                Categories.SESSION, backend,
                Categories.PLAYER_IDENTITY, backend,
                Categories.SETTING, backend);
        store = new DataStoreImpl(new CategoryRouter(routing), WritePathConfig.defaults(), LOG);
        store.start();
        registry = new CheckRegistry(new InMemoryPersistence());
        reachId = registry.intern("combat.reach", "Reach");
        timerId = registry.intern("movement.timer", "Timer");
        service = new HistoryServiceImpl(store, registry, 3, 30_000L);
    }

    @AfterEach
    void teardown() {
        store.flushAndClose(2_000);
    }

    @Test
    void emptyHistoryReturnsEmptyPage() throws Exception {
        UUID player = UUID.randomUUID();
        Page<SessionSummary> page = service.listSessions(player, null, 0)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(page.items().isEmpty());
        assertNull(page.nextCursor());
    }

    @Test
    void sessionListReturnsOrdinalsAndCounts() throws Exception {
        UUID player = UUID.randomUUID();
        UUID s1Id = UUID.randomUUID();
        UUID s2Id = UUID.randomUUID();
        submitSession(s1Id, player, 1_000_000L, 1_030_000L);
        submitSession(s2Id, player, 2_000_000L, 2_050_000L);
        submitViolation(s1Id, player, reachId, 1_001_000L);
        submitViolation(s1Id, player, timerId, 1_005_000L);
        submitViolation(s2Id, player, reachId, 2_010_000L);
        awaitEmpty();

        Page<SessionSummary> page = service.listSessions(player, null, 10)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(2, page.items().size());

        // Newest first: s2 (ordinal 2, 1 violation), s1 (ordinal 1, 2 violations).
        SessionSummary newest = page.items().get(0);
        SessionSummary oldest = page.items().get(1);
        assertEquals(2, newest.pageOrdinal());
        assertEquals(s2Id, newest.sessionId());
        assertEquals(1L, newest.violationCount());
        assertEquals(1, oldest.pageOrdinal());
        assertEquals(s1Id, oldest.sessionId());
        assertEquals(2L, oldest.violationCount());
    }

    @Test
    void sessionDetailBucketsByInterval() throws Exception {
        UUID player = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        submitSession(sid, player, 1_000_000L, 1_090_000L);
        // Bucket 0 (0-29s): two Reach, one Timer
        submitViolation(sid, player, reachId, 1_000_000L);
        submitViolation(sid, player, reachId, 1_010_000L);
        submitViolation(sid, player, timerId, 1_020_000L);
        // Bucket 1 (30-59s): one Reach
        submitViolation(sid, player, reachId, 1_040_000L);
        awaitEmpty();

        SessionDetail detail = service.getSessionDetail(player, sid)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNotNull(detail);
        assertEquals(30_000L, detail.bucketSizeMs());
        assertEquals(2, detail.buckets().size());

        CheckBucket first = detail.buckets().get(0);
        assertEquals(0L, first.bucketStartOffsetMs());
        assertEquals(2, first.checks().size());

        CheckBucket second = detail.buckets().get(1);
        assertEquals(30_000L, second.bucketStartOffsetMs());
        assertEquals(1, second.checks().size());
        assertEquals("Reach", second.checks().get(0).displayName());
    }

    @Test
    void violationEntriesCarryResolvedNamesAndOffsets() throws Exception {
        UUID player = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        submitSession(sid, player, 0L, 1_000L);
        submitViolation(sid, player, reachId, 500L);
        submitViolation(sid, player, timerId, 600L);
        awaitEmpty();

        SessionDetail detail = service.getSessionDetail(player, sid)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNotNull(detail);
        assertEquals(2, detail.violations().size());

        ViolationEntry first = detail.violations().get(0);
        assertEquals("Reach", first.displayName());
        assertEquals("combat.reach", first.stableKey());
        assertEquals(500L, first.offsetFromSessionStartMs());
    }

    @Test
    void sessionNotFoundReturnsNull() throws Exception {
        UUID player = UUID.randomUUID();
        SessionDetail detail = service.getSessionDetail(player, UUID.randomUUID())
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNull(detail);
    }

    @Test
    void sessionBelongingToDifferentPlayerReturnsNull() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        submitSession(sid, alice, 0L, 1_000L);
        awaitEmpty();
        SessionDetail detail = service.getSessionDetail(bob, sid)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNull(detail);
    }

    private void submitSession(UUID sid, UUID player, long start, long end) {
        store.submit(Categories.SESSION, populateSession(new SessionRecord(
                sid, player, "Prison", start, end,
                "3.1.0", "vanilla", "1.21.1", "Paper", List.of())));
    }

    private void submitViolation(UUID sid, UUID player, int checkId, long time) {
        store.submit(Categories.VIOLATION, e -> e
                .sessionId(sid).playerUuid(player).checkId(checkId)
                .vl(1.0).occurredEpochMs(time).verbose("v").verboseFormat(VerboseFormat.TEXT));
    }

    private static Consumer<SessionEvent> populateSession(SessionRecord r) {
        return e -> e
                .sessionId(r.sessionId()).playerUuid(r.playerUuid()).serverName(r.serverName())
                .startedEpochMs(r.startedEpochMs()).lastActivityEpochMs(r.lastActivityEpochMs())
                .grimVersion(r.grimVersion()).clientBrand(r.clientBrand())
                .clientVersionString(r.clientVersionString()).serverVersionString(r.serverVersionString())
                .replaceReplayClips(r.replayClips());
    }

    @SuppressWarnings("unused")
    private static Consumer<ViolationEvent> populateViolation(UUID sid, UUID player, int checkId, long time) {
        return e -> e
                .sessionId(sid).playerUuid(player).checkId(checkId)
                .vl(1.0).occurredEpochMs(time).verbose("v").verboseFormat(VerboseFormat.TEXT);
    }

    private void awaitEmpty() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (store.metrics().queuedCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        // Ring queued count can hit 0 before the consumer finishes processing the final
        // batch — give the consumer a short grace window so reads see the committed rows.
        Thread.sleep(50);
    }

    private static final class InMemoryPersistence implements CheckRegistry.CheckPersistence {
        private final Map<Integer, CheckRegistry.CheckRow> byId = new HashMap<>();
        private int next = 1;
        @Override public Iterable<CheckRegistry.CheckRow> loadAll() { return new ArrayList<>(byId.values()); }
        @Override public int insert(String stableKey, String display) {
            int id = next++;
            byId.put(id, new CheckRegistry.CheckRow(id, stableKey, display));
            return id;
        }
        @Override public void updateDisplay(int checkId, String display) {
            CheckRegistry.CheckRow prev = byId.get(checkId);
            if (prev != null) byId.put(checkId, new CheckRegistry.CheckRow(checkId, prev.stableKey(), display));
        }
    }
}
