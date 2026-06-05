package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRepairResult;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import ac.grim.grimac.api.storage.config.WaitStrategyType;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.backend.sqlite.SqliteBackendConfig;
import ac.grim.grimac.internal.storage.backend.sqlite.v2.SqliteBackendV2;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.checks.InMemoryCheckCatalogPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataStoreImplTest {

    @Test
    void directViolationSubmitMintsIdBeforeBackendReceivesEvent() throws Exception {
        CapturingViolationBackend backend = new CapturingViolationBackend();
        DataStoreImpl store = new DataStoreImpl(
                new CategoryRouter(Map.of(Categories.VIOLATION, backend)),
                new WritePathConfig(8, 1, 1L, 10_000L, 1_000L, WaitStrategyType.BLOCKING),
                Logger.getLogger("DataStoreImplTest"));
        store.start();

        try {
            store.submit(Categories.VIOLATION, event -> event
                    .sessionId(UUID.randomUUID())
                    .playerUuid(UUID.randomUUID())
                    .checkId(7)
                    .vl(1.0)
                    .occurredEpochMs(1_700_000_000_000L));

            assertTrue(backend.await(), "backend received violation event");
            assertNotNull(backend.id.get(), "DataStore submit path fills violation id");
        } finally {
            store.flushAndClose(1_000L);
        }
    }

    @Test
    void v2PlayerIdentityQueriesResolveNamesThroughCaseInsensitiveIndex(@TempDir Path tempDir) throws Exception {
        Logger logger = Logger.getLogger("DataStoreImplTest");
        SqliteBackendConfig cfg = SqliteBackendConfig.defaults("players-v2.db");
        SqliteBackendV2 backend = new SqliteBackendV2(cfg);
        backend.init(new TestBackendContext(cfg, tempDir, logger));

        Entity<UUID, PlayerIdentityEvent, PlayerIdentity> players = V2BuiltinKinds.players();
        StoreId storeId = StoreId.grim("grim_players");
        backend.adapterFor(players).orElseThrow().ensureStore(storeId, players);

        DataStoreImpl store = new DataStoreImpl(
                new CategoryRouter(Map.of()),
                new WritePathConfig(8, 1, 1L, 10_000L, 1_000L, WaitStrategyType.BLOCKING),
                logger);
        store.withV2Routes(V2Routes.builder()
                .register(Categories.PLAYER_IDENTITY, storeId, players, backend)
                .build());
        store.start();

        UUID player = UUID.randomUUID();
        try {
            store.submit(Categories.PLAYER_IDENTITY, event -> event
                    .uuid(player)
                    .currentName("John_Hydra")
                    .firstSeenEpochMs(1_700_000_000_000L)
                    .lastSeenEpochMs(1_700_000_000_500L));
            waitFor(() -> store.query(Categories.PLAYER_IDENTITY, Queries.getPlayerIdentity(player))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS)
                    .items().size() == 1);

            Page<PlayerIdentity> byName = store.query(Categories.PLAYER_IDENTITY,
                            Queries.getPlayerIdentityByName("john_hydra"))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, byName.items().size(), "case-insensitive exact name lookup");
            assertEquals(player, byName.items().get(0).uuid());

            Page<PlayerIdentity> byPrefix = store.query(Categories.PLAYER_IDENTITY,
                            Queries.listPlayersByNamePrefix("john", 10))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, byPrefix.items().size(), "case-insensitive prefix lookup");
            assertEquals(player, byPrefix.items().get(0).uuid());
        } finally {
            store.flushAndClose(1_000L);
            backend.close();
        }
    }

    @Test
    void inMemoryRepairRewritesLegacyHashCheckIdsAndStubVersions() throws Exception {
        InMemoryBackend backend = new InMemoryBackend();
        String stableKey = "grim.badpackets.invalid_interact_order";
        int catalogId = backend.checkCatalog().insert(
                stableKey, "BadPacketsM", "Invalid interact order", "0.0.0-stub", 1L);
        int legacyHashId = stableKey.hashCode();
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();

        backend.bulkImport(Categories.VIOLATION, List.of(new ViolationRecord(
                UUID.randomUUID(),
                session,
                player,
                legacyHashId,
                1.0,
                1_700_000_000_000L,
                "verbose".getBytes(StandardCharsets.UTF_8))));

        CheckCatalogRepairResult result = backend.repairCheckCatalog(
                Map.of(legacyHashId, catalogId),
                "3.0.152-test");

        assertEquals(1, result.mappingsApplied());
        assertEquals(1L, result.violationsUpdated());
        assertEquals(1L, result.catalogVersionsUpdated());
        Page<ViolationRecord> page = backend.read(
                Categories.VIOLATION,
                new Queries.ListViolationsInSession(session, 10, null));
        assertEquals(catalogId, page.items().get(0).checkId());
        CheckCatalogRow row = backend.checkCatalog().loadAll().iterator().next();
        assertEquals("3.0.152-test", row.introducedVersion());
    }

    private static void waitFor(ThrowingBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) return;
            } catch (Throwable t) {
                last = t;
            }
            Thread.sleep(25L);
        }
        if (last != null) {
            throw new AssertionError("condition did not become true", last);
        }
        throw new AssertionError("condition did not become true");
    }

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private record TestBackendContext(
            BackendConfig config,
            Path dataDirectory,
            Logger logger) implements BackendContext {
    }

    private static final class CapturingViolationBackend implements Backend {
        private final CountDownLatch seen = new CountDownLatch(1);
        private final AtomicReference<UUID> id = new AtomicReference<>();

        boolean await() throws InterruptedException {
            return seen.await(2, TimeUnit.SECONDS);
        }

        @Override public String id() { return "capturing"; }
        @Override public ApiVersion getApiVersion() { return ApiVersion.CURRENT; }
        @Override
        public EnumSet<Capability> capabilities() {
            return EnumSet.of(Capability.INDEXED_KV, Capability.TIMESERIES_APPEND, Capability.HISTORY);
        }
        @Override public Set<Category<?>> supportedCategories() { return Set.of(Categories.VIOLATION); }
        @Override public void init(BackendContext ctx) {}
        @Override public CheckCatalogPersistence checkCatalog() { return new InMemoryCheckCatalogPersistence(); }
        @Override
        public CheckCatalogRepairResult repairCheckCatalog(
                Map<Integer, Integer> legacyToCatalogCheckIds,
                String introducedVersionReplacement) {
            return new CheckCatalogRepairResult(legacyToCatalogCheckIds.size(), 0L, 0L);
        }
        @Override public void flush() {}
        @Override public void close() {}

        @Override
        @SuppressWarnings("unchecked")
        public <E> StorageEventHandler<E> eventHandlerFor(Category<E> cat) throws BackendException {
            if (cat != Categories.VIOLATION) throw new BackendException("unsupported category: " + cat.id());
            return (StorageEventHandler<E>) (StorageEventHandler<ViolationEvent>) (event, sequence, endOfBatch) -> {
                id.set(event.id());
                seen.countDown();
            };
        }

        @Override public <R> Page<R> read(Category<?> cat, Query<R> query) { return Page.empty(); }
        @Override public <E> void delete(Category<E> cat, DeleteCriteria criteria) {}
        @Override public long countViolationsInSession(UUID sessionId) { return 0L; }
    }
}
