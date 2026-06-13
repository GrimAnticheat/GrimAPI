package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import ac.grim.grimac.api.storage.event.CheckCatalogEvent;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataStoreCheckCatalogPersistenceTest {

    @Test
    void insertAllocatesLocallyAndSubmitsRoutedCatalogEvent() {
        CapturingDataStore store = new CapturingDataStore();
        DataStoreCheckCatalogPersistence persistence = new DataStoreCheckCatalogPersistence(
                List.of(new CheckCatalogRow(7, "existing", "Existing", null, "2.0", 1L)),
                store,
                Logger.getLogger("DataStoreCheckCatalogPersistenceTest"));

        int existing = persistence.insert("existing", "Existing", null, "2.0", 1L);
        int inserted = persistence.insert("new.check", "NewCheck", "desc", "3.0", 2L);

        assertEquals(7, existing);
        assertEquals(8, inserted);
        assertEquals(1, store.events.size(), "existing row is served from local cache");
        CheckCatalogEvent event = store.events.get(0);
        assertEquals("new.check", event.stableKey());
        assertEquals(8, event.checkId());
        assertEquals("NewCheck", event.display());
        assertEquals("desc", event.description());
        assertEquals("3.0", event.introducedVersion());
        assertEquals(2L, event.introducedAt());
    }

    @Test
    void upsertRejectsConflictingIdOrStableKey() {
        DataStoreCheckCatalogPersistence persistence = new DataStoreCheckCatalogPersistence(
                List.of(new CheckCatalogRow(5, "existing", "Existing", null, "2.0", 1L)),
                new CapturingDataStore(),
                Logger.getLogger("DataStoreCheckCatalogPersistenceTest"));

        assertThrows(IllegalStateException.class,
                () -> persistence.upsert(new CheckCatalogRow(6, "existing", "Other", null, "3.0", 2L)));
        assertThrows(IllegalStateException.class,
                () -> persistence.upsert(new CheckCatalogRow(5, "other", "Other", null, "3.0", 2L)));
    }

    @Test
    void updateDisplaySubmitsReplacementRow() {
        CapturingDataStore store = new CapturingDataStore();
        DataStoreCheckCatalogPersistence persistence = new DataStoreCheckCatalogPersistence(
                List.of(new CheckCatalogRow(4, "existing", "Old", "old", "2.0", 1L)),
                store,
                Logger.getLogger("DataStoreCheckCatalogPersistenceTest"));

        persistence.updateDisplayAndDescription(4, "New", "new");

        assertEquals(1, store.events.size());
        CheckCatalogEvent event = store.events.get(0);
        assertEquals("existing", event.stableKey());
        assertEquals(4, event.checkId());
        assertEquals("New", event.display());
        assertEquals("new", event.description());
        assertEquals("2.0", event.introducedVersion());
        assertEquals(1L, event.introducedAt());
    }

    private static final class CapturingDataStore implements DataStore {
        final List<CheckCatalogEvent> events = new ArrayList<>();

        @Override
        @SuppressWarnings("unchecked")
        public <E> void submit(Category<E> cat, Consumer<E> configurer) {
            if (cat != Categories.CHECK_CATALOG) {
                throw new AssertionError("unexpected category " + cat.id());
            }
            CheckCatalogEvent event = new CheckCatalogEvent();
            configurer.accept((E) event);
            events.add(event);
        }

        @Override
        public <R> CompletionStage<Page<R>> query(Category<?> cat, Query<R> query) {
            return CompletableFuture.completedFuture(Page.empty());
        }

        @Override
        public <R> CompletionStage<R> execute(Operation<R> op) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public <E> CompletionStage<Void> delete(Category<E> cat, DeleteCriteria criteria) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<DeletionReport> forgetPlayer(UUID uuid) {
            return CompletableFuture.completedFuture(DeletionReport.EMPTY);
        }

        @Override
        public CompletionStage<Long> countViolationsInSession(UUID sessionId) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletionStage<Long> countUniqueChecksInSession(UUID sessionId) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletionStage<Long> countSessionsByPlayer(UUID player) {
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public DataStoreMetrics metrics() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flushAndClose(long drainTimeoutMs) {
        }
    }
}
