package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.CheckCatalogRecord;
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
    void insertAllocatesLocallyAndPersistsRoutedCatalogRecord() {
        CapturingDataStore store = new CapturingDataStore();
        DataStoreCheckCatalogPersistence persistence = new DataStoreCheckCatalogPersistence(
                List.of(new CheckCatalogRow(7, "existing", "Existing", null, "2.0", 1L)),
                store,
                Logger.getLogger("DataStoreCheckCatalogPersistenceTest"));

        int existing = persistence.insert("existing", "Existing", null, "2.0", 1L);
        int inserted = persistence.insert("new.check", "NewCheck", "desc", "3.0", 2L);

        assertEquals(7, existing);
        assertEquals(8, inserted);
        assertEquals(1, store.records.size(), "existing row is served from local cache");
        CheckCatalogRecord record = store.records.get(0);
        assertEquals("new.check", record.stableKey());
        assertEquals(8, record.checkId());
        assertEquals("NewCheck", record.display());
        assertEquals("desc", record.description());
        assertEquals("3.0", record.introducedVersion());
        assertEquals(2L, record.introducedAt());
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
    void updateDisplayPersistsReplacementRow() {
        CapturingDataStore store = new CapturingDataStore();
        DataStoreCheckCatalogPersistence persistence = new DataStoreCheckCatalogPersistence(
                List.of(new CheckCatalogRow(4, "existing", "Old", "old", "2.0", 1L)),
                store,
                Logger.getLogger("DataStoreCheckCatalogPersistenceTest"));

        persistence.updateDisplayAndDescription(4, "New", "new");

        assertEquals(1, store.records.size());
        CheckCatalogRecord record = store.records.get(0);
        assertEquals("existing", record.stableKey());
        assertEquals(4, record.checkId());
        assertEquals("New", record.display());
        assertEquals("new", record.description());
        assertEquals("2.0", record.introducedVersion());
        assertEquals(1L, record.introducedAt());
    }

    private static final class CapturingDataStore implements DataStore {
        final List<CheckCatalogRecord> records = new ArrayList<>();

        @Override
        public <E> void submit(Category<E> cat, Consumer<E> configurer) {
            throw new AssertionError("unexpected ring submit for " + cat.id());
        }

        @Override
        public <R> CompletionStage<Page<R>> query(Category<?> cat, Query<R> query) {
            return CompletableFuture.completedFuture(Page.empty());
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> CompletionStage<R> execute(Operation<R> op) {
            if (op instanceof EntityOps.UpsertOp<?> upsert
                    && upsert.category() == Categories.CHECK_CATALOG
                    && upsert.record() instanceof CheckCatalogRecord record) {
                records.add(record);
                return CompletableFuture.completedFuture(null);
            }
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
