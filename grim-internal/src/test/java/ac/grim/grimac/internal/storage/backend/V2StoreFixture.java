package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WaitStrategyType;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoMigrationContext;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.client.MongoDatabase;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import ac.grim.grimac.internal.storage.core.V2BackendBootstrap;
import ac.grim.grimac.internal.storage.core.V2Routes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@link DataStoreImpl} routed to one V2 backend for sessions and violations, on
 * fresh store names, as Grim's lifecycle builds it. Rows are written straight through
 * the adapters' write handlers, so tests do not wait on the ring buffers.
 */
public final class V2StoreFixture implements AutoCloseable {

    public final DataStoreImpl store;
    public final V2Routes routes;
    public final String violationsTable;
    public final String sessionsTable;
    private final StorageEventHandler<ViolationEvent> violationWriter;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public V2StoreFixture(BackendV2 backend, String prefix, Logger logger) {
        violationsTable = SqlTestDatabases.uniqueTable(prefix + "_violations");
        sessionsTable = SqlTestDatabases.uniqueTable(prefix + "_sessions");
        Map<Category<?>, V2BackendBootstrap.Binding<?>> bindings = new LinkedHashMap<>();
        bindings.put(Categories.VIOLATION,
            new V2BackendBootstrap.Binding<>(StoreId.grim(violationsTable), V2BuiltinKinds.violations()));
        bindings.put(Categories.SESSION,
            new V2BackendBootstrap.Binding<>(StoreId.grim(sessionsTable), V2BuiltinKinds.sessions()));
        V2Routes.Builder builder = V2Routes.builder();
        V2BackendBootstrap.Result result = V2BackendBootstrap.install(
            bindings, backend, migrationContext(backend, logger), builder, logger);
        assertTrue(result.ok(), () -> String.join("\n", result.failures()));
        routes = builder.build();
        store = new DataStoreImpl(new CategoryRouter(Map.of()),
            new WritePathConfig(1024, 256, 50L, 10_000L, 2_000L, WaitStrategyType.BLOCKING), logger);
        store.withV2Routes(routes);
        store.start();
        V2Routes.Route route = (V2Routes.Route) routes.routeFor(Categories.VIOLATION);
        KindAdapter adapter = route.adapter();
        violationWriter = adapter.writeHandler(route.storeId(), route.kind(), Categories.VIOLATION);
    }

    private static MigrationContext migrationContext(BackendV2 backend, Logger logger) {
        return backend.unwrap(MongoDatabase.class)
            .<MigrationContext>map(db -> new MongoMigrationContext(db, logger))
            .orElseGet(() -> new MigrationContext() {});
    }

    public void session(UUID sessionId, UUID player, long startedAt, long lastActivity) throws Exception {
        store.execute(new EntityOps.UpsertOp<>(Categories.SESSION, new SessionRecord(sessionId, player, "srv",
                startedAt, lastActivity, lastActivity, "test", "vanilla", 767, "1.21", null, List.of())))
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    /** Writes violations in batches; {@code last} marks the end of a batch. */
    public void violation(UUID id, UUID sessionId, UUID player, int checkId, long occurredAt, boolean last) throws Exception {
        violationWriter.onEvent(new ViolationEvent().id(id).sessionId(sessionId).playerUuid(player).checkId(checkId)
            .vl(1.0).occurredEpochMs(occurredAt).verboseFormat(VerboseFormat.TEXT), 0L, last);
    }

    @Override public void close() {
        store.flushAndClose(2_000L);
    }
}
