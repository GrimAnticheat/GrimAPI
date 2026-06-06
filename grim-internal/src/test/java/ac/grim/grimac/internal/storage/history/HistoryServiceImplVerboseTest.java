package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.DataStoreMetrics;
import ac.grim.grimac.api.storage.DeletionReport;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.api.storage.verbose.VerboseBuf;
import ac.grim.grimac.api.storage.verbose.VerboseFormatter;
import ac.grim.grimac.api.storage.verbose.VerboseRenderContext;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.api.storage.verbose.VerboseSink;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.InMemoryCheckCatalogPersistence;
import ac.grim.grimac.internal.storage.verbose.VerboseManifest;
import ac.grim.grimac.internal.storage.verbose.VerboseRegistry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HistoryServiceImplVerboseTest {

    private static final VerboseRenderContext UNKNOWN_CONTEXT = new VerboseRenderContext(-1, null);

    @Test
    void renderVerboseUsesTextFallbackForVersionZero() {
        HistoryServiceImpl history = history(new FixedVerboseRegistry(null));
        byte[] text = "legacy verbose".getBytes(StandardCharsets.UTF_8);

        String rendered = history.renderVerbose(
                text,
                startup(VerboseManifest.textOnly(VerboseManifest.FLAVOR_V2_PUBLIC)),
                42,
                UNKNOWN_CONTEXT);

        assertEquals("legacy verbose", rendered);
    }

    @Test
    void renderVerboseUsesGenericDecodeForVersionOneWhenSchemaIsPresent() {
        VerboseSchema schema = VerboseSchema.of(1, "offset:f64", "flagId:vi");
        HistoryServiceImpl history = history(new FixedVerboseRegistry(VerboseSchema.decodeLayout(schema.layoutBytes())));
        byte[] payload = schema.write(new VerboseBuf()).f64(1.25).vi(7).toByteArray();

        String rendered = history.renderVerbose(
                payload,
                startup(VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC, Map.of(42, 1))),
                42,
                UNKNOWN_CONTEXT);

        assertEquals("offset=1.25, flagId=7", rendered);
    }

    @Test
    void renderVerboseUsesPlaceholderWhenSchemaIsAbsent() {
        HistoryServiceImpl history = history(new FixedVerboseRegistry(null));

        String rendered = history.renderVerbose(
                new byte[] {0x01, 0x02},
                startup(VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC, Map.of(42, 1))),
                42,
                UNKNOWN_CONTEXT);

        assertEquals("[binary verbose v1, schema unavailable] 0x0102", rendered);
    }

    @Test
    void renderVerbosePassesContextToCustomFormatter() {
        VerboseFormatter formatter = new VerboseFormatter() {
            @Override public int version() { return 1; }

            @Override
            public void render(VerboseBuf in, VerboseRenderContext ctx, VerboseSink out) {
                out.key("clientVersion");
                out.num(ctx.clientVersionPvn());
            }
        };
        HistoryServiceImpl history = history(new FixedVerboseRegistry(null, formatter));

        String rendered = history.renderVerbose(
                new byte[] {0x01},
                startup(VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC, Map.of(42, 1))),
                42,
                new VerboseRenderContext(772, "1.21.11"));

        assertEquals("clientVersion=772", rendered);
    }

    private static HistoryServiceImpl history(VerboseRegistry registry) {
        InMemoryCheckCatalogPersistence persistence = new InMemoryCheckCatalogPersistence();
        persistence.upsert(new CheckCatalogRow(42, "movement.vertical", "Vertical", null, null, 1L));
        CheckRegistry checks = new CheckRegistry(persistence);
        checks.reload();
        return new HistoryServiceImpl(new NoopStore(), checks, 10, 1_000L)
                .withVerboseRegistry(registry);
    }

    private static ServerStartupRecord startup(byte[] manifest) {
        return new ServerStartupRecord(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "test",
                null,
                null,
                null,
                1L,
                1L,
                ServerStartupRecord.OPEN,
                null,
                manifest);
    }

    private record FixedVerboseRegistry(VerboseSchema.Layout layout, VerboseFormatter formatter) implements VerboseRegistry {
        private FixedVerboseRegistry(VerboseSchema.Layout layout) {
            this(layout, null);
        }

        @Override public void register(String stableKey, VerboseSchema schema) {}
        @Override public void registerFormatter(String stableKey, VerboseFormatter formatter) {}
        @Override public Map<Integer, Integer> checkIdVersions(CheckRegistry checks) { return Map.of(); }
        @Override public VerboseFormatter codeFormatter(int flavor, int checkId, int version) { return formatter; }
        @Override public VerboseSchema.Layout layout(int flavor, int checkId, int version) { return layout; }
    }

    private static final class NoopStore implements DataStore {
        @Override public <E> void submit(Category<E> cat, Consumer<E> configurer) {}
        @Override public <R> CompletionStage<Page<R>> query(Category<?> cat, Query<R> query) {
            return CompletableFuture.completedStage(Page.empty());
        }
        @Override public <R> CompletionStage<R> execute(Operation<R> op) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(new UnsupportedOperationException());
            return failed;
        }
        @Override public <E> CompletionStage<Void> delete(Category<E> cat, DeleteCriteria criteria) {
            return CompletableFuture.completedStage(null);
        }
        @Override public CompletionStage<DeletionReport> forgetPlayer(UUID uuid) {
            return CompletableFuture.completedStage(DeletionReport.EMPTY);
        }
        @Override public CompletionStage<Long> countViolationsInSession(UUID sessionId) {
            return CompletableFuture.completedStage(0L);
        }
        @Override public CompletionStage<Long> countUniqueChecksInSession(UUID sessionId) {
            return CompletableFuture.completedStage(0L);
        }
        @Override public CompletionStage<Long> countSessionsByPlayer(UUID player) {
            return CompletableFuture.completedStage(0L);
        }
        @Override public DataStoreMetrics metrics() {
            return new DataStoreMetrics() {
                @Override public long queuedCount() { return 0; }
                @Override public long submittedTotal() { return 0; }
                @Override public long droppedOnOverflowTotal() { return 0; }
                @Override public long droppedOnErrorTotal() { return 0; }
                @Override public long writeBatchLatencyMsEma() { return 0; }
                @Override public long readLatencyMsEma() { return 0; }
            };
        }
        @Override public void flushAndClose(long drainTimeoutMs) {}
    }
}
