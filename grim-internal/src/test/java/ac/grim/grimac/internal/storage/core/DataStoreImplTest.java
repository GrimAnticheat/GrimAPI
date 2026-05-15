package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.config.WaitStrategyType;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.checks.InMemoryCheckCatalogPersistence;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

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
