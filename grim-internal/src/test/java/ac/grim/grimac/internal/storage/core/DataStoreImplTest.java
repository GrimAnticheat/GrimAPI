package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DataStoreImplTest {

    private static final Logger LOG = Logger.getLogger("DataStoreImplTest");

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
        CategoryRouter router = new CategoryRouter(routing);
        store = new DataStoreImpl(router, WritePathConfig.defaults(), LOG);
        store.start();
    }

    @AfterEach
    void tearDown() {
        store.flushAndClose(1_000);
    }

    @Test
    void submitThenQueryRoundTrips() throws Exception {
        UUID player = UUID.randomUUID();
        SessionRecord s = new SessionRecord(UUID.randomUUID(), player, "Prison", 1000, 1500,
                "3.1.0", "vanilla", "1.21.1", "Paper", List.of());
        store.submit(Categories.SESSION, s);
        awaitEmptyQueue();
        Page<SessionRecord> page = store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, 10, null))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(1, page.items().size());
        assertEquals(s.sessionId(), page.items().get(0).sessionId());
    }

    @Test
    void countViolationsInSessionViaDataStore() throws Exception {
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        for (int i = 0; i < 42; i++) {
            store.submit(Categories.VIOLATION,
                    new ViolationRecord(0, session, player, 1, 1.0, i * 10L, "v", VerboseFormat.TEXT));
        }
        awaitEmptyQueue();
        long count = store.countViolationsInSession(session).toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(42, count);
    }

    @Test
    void forgetPlayerReportsDeletionCounts() throws Exception {
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        store.submit(Categories.SESSION, new SessionRecord(session, player, "Prison",
                1000, 1000, "3.1.0", "vanilla", "1.21.1", "Paper", List.of()));
        store.submit(Categories.PLAYER_IDENTITY, new PlayerIdentity(player, "Alice", 1000, 1000));
        for (int i = 0; i < 7; i++) {
            store.submit(Categories.VIOLATION,
                    new ViolationRecord(0, session, player, 1, 1.0, 1000 + i, "v", VerboseFormat.TEXT));
        }
        awaitEmptyQueue();

        DeletionReport report = ((DataStore) store).forgetPlayer(player)
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertNotNull(report);
        assertEquals(1, report.sessionsDeleted());
        assertEquals(7, report.violationsDeleted());
        assertEquals(1, report.identitiesDeleted());

        // verify gone
        Page<SessionRecord> page = store.query(Categories.SESSION, Queries.listSessionsByPlayer(player, 10, null))
                .toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(page.items().isEmpty());
    }

    @Test
    void metricsReflectSubmissions() throws Exception {
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        for (int i = 0; i < 20; i++) {
            store.submit(Categories.VIOLATION,
                    new ViolationRecord(0, session, player, 1, 1.0, i, "v", VerboseFormat.TEXT));
        }
        awaitEmptyQueue();
        assertEquals(20, store.metrics().submittedTotal());
        assertEquals(0, store.metrics().droppedOnOverflowTotal());
    }

    private void awaitEmptyQueue() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (store.metrics().queuedCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }
}
