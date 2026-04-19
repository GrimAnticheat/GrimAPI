package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.history.CheckBucket;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.history.ViolationEntry;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
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
        store.flushAndClose(500);
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
        SessionRecord s1 = session(player, 1_000_000L, 1_030_000L);
        SessionRecord s2 = session(player, 2_000_000L, 2_050_000L);
        store.submit(Categories.SESSION, s1);
        store.submit(Categories.SESSION, s2);
        store.submit(Categories.VIOLATION, violation(s1.sessionId(), player, reachId, 1_001_000L));
        store.submit(Categories.VIOLATION, violation(s1.sessionId(), player, timerId, 1_005_000L));
        store.submit(Categories.VIOLATION, violation(s2.sessionId(), player, reachId, 2_010_000L));
        awaitEmpty();

        Page<SessionSummary> page = service.listSessions(player, null, 10)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(2, page.items().size());

        // Newest first: s2 (ordinal 2, 1 violation), s1 (ordinal 1, 2 violations).
        SessionSummary newest = page.items().get(0);
        SessionSummary oldest = page.items().get(1);
        assertEquals(2, newest.pageOrdinal());
        assertEquals(s2.sessionId(), newest.sessionId());
        assertEquals(1L, newest.violationCount());
        assertEquals(1, oldest.pageOrdinal());
        assertEquals(s1.sessionId(), oldest.sessionId());
        assertEquals(2L, oldest.violationCount());
    }

    @Test
    void sessionDetailBucketsByInterval() throws Exception {
        UUID player = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        SessionRecord s = new SessionRecord(sid, player, "Prison",
                1_000_000L, 1_090_000L,
                "3.1.0", "vanilla", "1.21.1", "Paper", List.of());
        store.submit(Categories.SESSION, s);
        // Bucket 0 (0-29s): two Reach, one Timer
        store.submit(Categories.VIOLATION, violation(sid, player, reachId, 1_000_000L));
        store.submit(Categories.VIOLATION, violation(sid, player, reachId, 1_010_000L));
        store.submit(Categories.VIOLATION, violation(sid, player, timerId, 1_020_000L));
        // Bucket 1 (30-59s): one Reach
        store.submit(Categories.VIOLATION, violation(sid, player, reachId, 1_040_000L));
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
        store.submit(Categories.SESSION, new SessionRecord(sid, player, "Prison",
                0L, 1_000L, "3.1.0", "vanilla", "1.21.1", "Paper", List.of()));
        store.submit(Categories.VIOLATION, violation(sid, player, reachId, 500L));
        store.submit(Categories.VIOLATION, violation(sid, player, timerId, 600L));
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
        store.submit(Categories.SESSION, new SessionRecord(sid, alice, "Prison",
                0L, 1_000L, "3.1.0", "vanilla", "1.21.1", "Paper", List.of()));
        awaitEmpty();
        SessionDetail detail = service.getSessionDetail(bob, sid)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNull(detail);
    }

    private SessionRecord session(UUID player, long start, long end) {
        return new SessionRecord(UUID.randomUUID(), player, "Prison", start, end,
                "3.1.0", "vanilla", "1.21.1", "Paper", List.of());
    }

    private ViolationRecord violation(UUID sid, UUID player, int checkId, long time) {
        return new ViolationRecord(0, sid, player, checkId, 1.0, time, "v", VerboseFormat.TEXT);
    }

    private void awaitEmpty() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (store.metrics().queuedCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
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
