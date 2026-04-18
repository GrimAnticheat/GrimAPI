package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.history.HistoryService;
import ac.grim.grimac.api.storage.history.RenderOptions;
import ac.grim.grimac.api.storage.history.RenderedHistoryLine;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void emptyHistoryRendersFriendlyMessage() throws Exception {
        UUID player = UUID.randomUUID();
        HistoryService.SessionListResult res = service.renderSessionList(player, null, 0)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, res.lines().size());
        String combined = flatten(res.lines().get(0));
        assertTrue(combined.contains("No session history"));
    }

    @Test
    void sessionListRendersHeaderPlusPerSessionLine() throws Exception {
        UUID player = UUID.randomUUID();
        SessionRecord s1 = session(player, 1_000_000L, 1_030_000L);
        SessionRecord s2 = session(player, 2_000_000L, 2_050_000L);
        store.submit(Categories.SESSION, s1);
        store.submit(Categories.SESSION, s2);
        store.submit(Categories.VIOLATION, violation(s1.sessionId(), player, reachId, 1_001_000L));
        store.submit(Categories.VIOLATION, violation(s1.sessionId(), player, timerId, 1_005_000L));
        store.submit(Categories.VIOLATION, violation(s2.sessionId(), player, reachId, 2_010_000L));
        awaitEmpty();

        HistoryService.SessionListResult res = service.renderSessionList(player, null, 10)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(3, res.lines().size(), "header + 2 sessions");
        assertTrue(flatten(res.lines().get(0)).contains("Showing session history"));
        // Newest session shown first → ordinal N=2 ... oldest = 1.
        assertTrue(flatten(res.lines().get(1)).contains("Session 2"));
        assertTrue(flatten(res.lines().get(2)).contains("Session 1"));
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

        List<RenderedHistoryLine> lines = service.renderSessionDetail(player, sid,
                        new RenderOptions(false, 30_000L))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        // Header lines + violations header + 2 bucket lines
        assertTrue(lines.stream().anyMatch(l -> flatten(l).contains("session details")));
        long bucketLines = lines.stream().filter(l -> flatten(l).startsWith("- ")).count();
        assertEquals(2, bucketLines, "two buckets expected");
    }

    @Test
    void detailedFlagEmitsPerViolationLines() throws Exception {
        UUID player = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        store.submit(Categories.SESSION, new SessionRecord(sid, player, "Prison",
                0L, 1_000L, "3.1.0", "vanilla", "1.21.1", "Paper", List.of()));
        store.submit(Categories.VIOLATION, violation(sid, player, reachId, 500L));
        store.submit(Categories.VIOLATION, violation(sid, player, timerId, 600L));
        awaitEmpty();
        List<RenderedHistoryLine> lines = service.renderSessionDetail(player, sid,
                        new RenderOptions(true, 30_000L))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        boolean anyDetail = lines.stream().anyMatch(l -> flatten(l).startsWith("    "));
        assertTrue(anyDetail, "detailed mode should produce indented per-violation lines");
    }

    @Test
    void sessionNotFoundReturnsSingleLine() throws Exception {
        UUID player = UUID.randomUUID();
        List<RenderedHistoryLine> lines = service.renderSessionDetail(player, UUID.randomUUID(), null)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, lines.size());
        assertTrue(flatten(lines.get(0)).contains("not found"));
    }

    @Test
    void sessionBelongingToDifferentPlayerIsRefused() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID sid = UUID.randomUUID();
        store.submit(Categories.SESSION, new SessionRecord(sid, alice, "Prison",
                0L, 1_000L, "3.1.0", "vanilla", "1.21.1", "Paper", List.of()));
        awaitEmpty();
        List<RenderedHistoryLine> lines = service.renderSessionDetail(bob, sid, null)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertFalse(lines.isEmpty());
        assertTrue(flatten(lines.get(0)).contains("not belong"));
    }

    private SessionRecord session(UUID player, long start, long end) {
        return new SessionRecord(UUID.randomUUID(), player, "Prison", start, end,
                "3.1.0", "vanilla", "1.21.1", "Paper", List.of());
    }

    private ViolationRecord violation(UUID sid, UUID player, int checkId, long time) {
        return new ViolationRecord(0, sid, player, checkId, 1.0, time, "v", VerboseFormat.TEXT);
    }

    private String flatten(RenderedHistoryLine line) {
        StringBuilder sb = new StringBuilder();
        for (RenderedHistoryLine.Segment seg : line.segments()) flatten(seg, sb);
        return sb.toString();
    }

    private void flatten(RenderedHistoryLine.Segment seg, StringBuilder sb) {
        if (seg instanceof RenderedHistoryLine.Segment.Literal l) sb.append(l.text());
        else if (seg instanceof RenderedHistoryLine.Segment.Styled s) sb.append(s.text());
        else if (seg instanceof RenderedHistoryLine.Segment.Hover h) flatten(h.visible(), sb);
        else if (seg instanceof RenderedHistoryLine.Segment.CheckRef c) sb.append(c.displayName());
        else if (seg instanceof RenderedHistoryLine.Segment.Duration d) sb.append(d.ms()).append("ms");
        else if (seg instanceof RenderedHistoryLine.Segment.Timestamp t) sb.append("<ts>");
        else if (seg instanceof RenderedHistoryLine.Segment.PlayerRef p) sb.append("<p:").append(p.uuid()).append(">");
        else if (seg instanceof RenderedHistoryLine.Segment.ClickCommand c) flatten(c.visible(), sb);
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
