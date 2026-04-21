package ac.grim.grimac.internal.storage.retention;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.RetentionRule;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class RetentionSweeperTest {

    private InMemoryBackend backend;
    private DataStoreImpl store;

    @BeforeEach
    void setup() {
        backend = new InMemoryBackend();
        Map<Category<?>, Backend> routing = Map.of(
                Categories.VIOLATION, backend,
                Categories.SESSION, backend,
                Categories.PLAYER_IDENTITY, backend,
                Categories.SETTING, backend);
        store = new DataStoreImpl(new CategoryRouter(routing), WritePathConfig.defaults(), Logger.getLogger("rs"));
        store.start();
    }

    @AfterEach
    void teardown() {
        store.flushAndClose(500);
    }

    @Test
    void sweepsSessionsOlderThanMaxAge() throws Exception {
        UUID player = UUID.randomUUID();
        long now = System.currentTimeMillis();
        SessionRecord old = new SessionRecord(UUID.randomUUID(), player, "Prison",
                now - (100L * 24 * 60 * 60 * 1000), now - (100L * 24 * 60 * 60 * 1000),
                "3.1.0", "vanilla", 767, "Paper", List.of());
        SessionRecord recent = new SessionRecord(UUID.randomUUID(), player, "Prison",
                now - (5L * 24 * 60 * 60 * 1000), now - (5L * 24 * 60 * 60 * 1000),
                "3.1.0", "vanilla", 767, "Paper", List.of());
        backend.writeRecordsDirect(Categories.SESSION, List.of(old, recent));

        RetentionSweeper sweeper = new RetentionSweeper(store,
                Map.of(Categories.SESSION, new RetentionRule(true, 90)), Logger.getLogger("rs"));
        sweeper.sweepOnce();
        Page<SessionRecord> page = store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, 10, null))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, page.items().size(), "only recent should survive");
        assertEquals(recent.sessionId(), page.items().get(0).sessionId());
    }

    @Test
    void disabledRuleDoesNothing() throws Exception {
        UUID player = UUID.randomUUID();
        long now = System.currentTimeMillis();
        SessionRecord old = new SessionRecord(UUID.randomUUID(), player, "Prison",
                now - (1000L * 24 * 60 * 60 * 1000), now - (1000L * 24 * 60 * 60 * 1000),
                "3.1.0", "vanilla", 767, "Paper", List.of());
        backend.writeRecordsDirect(Categories.SESSION, List.of(old));

        RetentionSweeper sweeper = new RetentionSweeper(store,
                Map.of(Categories.SESSION, RetentionRule.DISABLED), Logger.getLogger("rs"));
        sweeper.sweepOnce();
        Page<SessionRecord> page = store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, 10, null))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, page.items().size(), "disabled rule should not sweep");
    }
}
