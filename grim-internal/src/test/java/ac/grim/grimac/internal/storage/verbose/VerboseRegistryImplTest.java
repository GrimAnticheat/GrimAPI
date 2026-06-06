package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.model.VerboseSchemaRecord;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class VerboseRegistryImplTest {

    @Test
    void layoutRetriesAfterTransientSchemaLookupFailure() {
        VerboseSchema schema = VerboseSchema.of("offset:f64", "ok:bool");
        VerboseSchemaRecord record = new VerboseSchemaRecord(
                VerboseSchemaRecord.keyOf(2, 42, schema.version()),
                2,
                42,
                schema.version(),
                schema.layoutBytes(),
                1L);
        FlakyStore store = new FlakyStore(record);
        VerboseRegistryImpl registry = new VerboseRegistryImpl(store, emptyChecks(), 1);

        assertNull(registry.layout(2, 42, schema.version()));
        VerboseSchema.Layout layout = registry.layout(2, 42, schema.version());

        assertNotNull(layout);
        assertEquals(schema.fields(), layout.fields());
        assertEquals(2, store.executeCalls());
    }

    private static @NotNull CheckRegistry emptyChecks() {
        return new CheckRegistry(new CheckCatalogPersistence() {
            @Override public Iterable<CheckCatalogRow> loadAll() { return List.of(); }
            @Override public int insert(
                    String stableKey,
                    String display,
                    String description,
                    String introducedVersion,
                    long introducedAt) {
                throw new UnsupportedOperationException();
            }
            @Override public void upsert(CheckCatalogRow row) { throw new UnsupportedOperationException(); }
            @Override public void updateDisplayAndDescription(int checkId, String display, String description) {
                throw new UnsupportedOperationException();
            }
        });
    }

    private static final class FlakyStore implements DataStore {
        private final VerboseSchemaRecord record;
        private final AtomicInteger executeCalls = new AtomicInteger();

        private FlakyStore(@NotNull VerboseSchemaRecord record) {
            this.record = record;
        }

        private int executeCalls() {
            return executeCalls.get();
        }

        @Override
        public <E> void submit(@NotNull Category<E> cat, @NotNull Consumer<E> configurer) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("removal")
        public @NotNull <R> CompletionStage<Page<R>> query(@NotNull Category<?> cat, @NotNull Query<R> query) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public @NotNull <R> CompletionStage<R> execute(@NotNull Operation<R> op) {
            if (executeCalls.incrementAndGet() == 1) {
                throw new RuntimeException("transient");
            }
            return (CompletionStage<R>) CompletableFuture.completedFuture(Optional.of(record));
        }

        @Override
        public @NotNull <E> CompletionStage<Void> delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull CompletionStage<DeletionReport> forgetPlayer(@NotNull UUID uuid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull CompletionStage<Long> countViolationsInSession(@NotNull UUID sessionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull CompletionStage<Long> countUniqueChecksInSession(@NotNull UUID sessionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull CompletionStage<Long> countSessionsByPlayer(@NotNull UUID player) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull DataStoreMetrics metrics() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flushAndClose(long drainTimeoutMs) {
        }
    }
}
