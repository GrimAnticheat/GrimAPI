package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.event.PlayerIdentityEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoBackendV2;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoMigrationContext;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link V2BackendBootstrap}. Verifies the full
 * end-to-end flow the plugin's {@code DataStoreLifecycle} will use:
 * <ol>
 *   <li>Build + init a v2 backend.</li>
 *   <li>Pass a Category → Binding map + V2Routes.Builder + a real
 *       MigrationContext to {@link V2BackendBootstrap#install}.</li>
 *   <li>Verify routes are populated.</li>
 *   <li>Use the routes to dispatch an Operation through DataStoreImpl-
 *       shaped code (here we go straight to the adapter to avoid spinning
 *       up the full ring registry).</li>
 *   <li>Verify the read returns the migrated v7-shaped data.</li>
 * </ol>
 *
 * <p>Gated on {@code GRIM_V2_SMOKE_DB} so the regular test suite stays
 * self-contained. Set the env var to the populated test database name
 * (e.g. {@code grim_storage_test_19_4}) to exercise.
 */
@DisplayName("v2 backend bootstrap install pass")
class V2BackendBootstrapTest {

    private static final String MONGO_CS = "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin";

    @Test
    @DisplayName("install + route dispatch against legacy v6 fixture")
    void installAndDispatch() throws Exception {
        String dbName = System.getProperty("GRIM_V2_SMOKE_DB", System.getenv("GRIM_V2_SMOKE_DB"));
        Assumptions.assumeTrue(dbName != null && !dbName.isEmpty(),
            "set GRIM_V2_SMOKE_DB to the populated test database (e.g. grim_storage_test_19_4)");
        assumeReachable("localhost", 27017);

        Logger logger = Logger.getLogger("V2BackendBootstrapTest");

        MongoBackendConfig cfg = new MongoBackendConfig(
            MONGO_CS, dbName, 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        backend.init(new BackendContext() {
            @Override public Logger logger() { return logger; }
            @Override public Path dataDirectory() { return Path.of("/tmp"); }
            @Override public BackendConfig config() { return cfg; }
        });

        try (MongoClient probe = MongoClients.create(MONGO_CS)) {
            MongoDatabase db = probe.getDatabase(dbName);
            MongoMigrationContext mctx = new MongoMigrationContext(db, logger);

            // Bindings cover the three categories with legacy Category
            // constants. Map insertion order = run order — keep
            // violations first so its timeseries migration runs before
            // entity migrations attach indexes to renamed collections.
            EventStream<ViolationEvent, ViolationRecord> violationsKind = V2BuiltinKinds.violations();
            Entity<UUID, SessionEvent, SessionRecord> sessionsKind = V2BuiltinKinds.sessions();
            Entity<UUID, PlayerIdentityEvent, PlayerIdentity> playersKind = V2BuiltinKinds.players();

            Map<Category<?>, V2BackendBootstrap.Binding<?>> bindings = new LinkedHashMap<>();
            bindings.put(Categories.VIOLATION,
                new V2BackendBootstrap.Binding<>(StoreId.grim("grim_violations"), violationsKind));
            bindings.put(Categories.SESSION,
                new V2BackendBootstrap.Binding<>(StoreId.grim("grim_sessions"), sessionsKind));
            bindings.put(Categories.PLAYER_IDENTITY,
                new V2BackendBootstrap.Binding<>(StoreId.grim("grim_players"), playersKind));

            V2Routes.Builder routesB = V2Routes.builder();
            V2BackendBootstrap.Result result = V2BackendBootstrap.install(
                bindings, backend, mctx, routesB, logger);

            if (!result.ok()) {
                org.junit.jupiter.api.Assertions.fail(
                    "bootstrap failures:\n  - " + String.join("\n  - ", result.failures()));
            }

            V2Routes routes = routesB.build();
            assertTrue(routes.contains(Categories.VIOLATION),  "VIOLATION route registered");
            assertTrue(routes.contains(Categories.SESSION),    "SESSION route registered");
            assertTrue(routes.contains(Categories.PLAYER_IDENTITY), "PLAYER_IDENTITY route registered");

            // Dispatch via the route — same code path DataStoreImpl.execute
            // takes internally. Verifies the (category → adapter + storeId
            // + kind) triple resolves and the read works on the migrated
            // v7-shaped data.
            verifyDispatch(routes, db, sessionsKind, playersKind, logger);

            // Schema markers updated post-migration.
            org.bson.Document meta = db.getCollection("grim_meta").find().first();
            assertNotNull(meta, "grim_meta should exist post-migration");
        } finally {
            backend.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void verifyDispatch(V2Routes routes, MongoDatabase db,
                                       Entity<UUID, ?, SessionRecord> sessionsKind,
                                       Entity<UUID, ?, PlayerIdentity> playersKind,
                                       Logger logger) throws Exception {
        // Sessions: pick any UUID from the migrated collection, dispatch
        // a GetByIdOp through the route, expect a SessionRecord back.
        org.bson.Document anySession = db.getCollection("grim_sessions").find().limit(1).first();
        assertNotNull(anySession, "fixture should have at least one session");
        UUID sessionId = extractUuid(anySession.get("_id"));

        V2Routes.Route sessionRoute = (V2Routes.Route) routes.routeFor(Categories.SESSION);
        assertNotNull(sessionRoute, "SESSION route should be present");

        EntityOps.GetByIdOp op = new EntityOps.GetByIdOp(Categories.SESSION, sessionId);
        Optional got = (Optional) sessionRoute.adapter().execute(
            sessionRoute.storeId(), sessionRoute.kind(), op);
        assertTrue(got.isPresent(),
            "v2 dispatch via route must decode legacy session " + sessionId);
        logger.info(() -> "dispatch ok: session " + sessionId + " round-tripped via route");

        // Players: same pattern.
        org.bson.Document anyPlayer = db.getCollection("grim_players").find().limit(1).first();
        if (anyPlayer != null) {
            UUID playerId = extractUuid(anyPlayer.get("_id"));
            V2Routes.Route playerRoute = (V2Routes.Route) routes.routeFor(Categories.PLAYER_IDENTITY);
            EntityOps.GetByIdOp pop = new EntityOps.GetByIdOp(Categories.PLAYER_IDENTITY, playerId);
            Optional pgot = (Optional) playerRoute.adapter().execute(
                playerRoute.storeId(), playerRoute.kind(), pop);
            assertTrue(pgot.isPresent(),
                "v2 dispatch via route must decode legacy player " + playerId);
            logger.info(() -> "dispatch ok: player " + playerId + " round-tripped via route");
        }

        // Violations: exercise EventStream dispatch path via CountOp on
        // a known session_id from the sessions collection. The legacy
        // fixture wrote violations partitioned by session_id, so a
        // CountOp on the migrated timeseries should return a non-zero
        // count for any session that produced violations.
        V2Routes.Route violationRoute = (V2Routes.Route) routes.routeFor(Categories.VIOLATION);
        assertNotNull(violationRoute, "VIOLATION route should be present");
        // Pick the session UUID we already extracted above.
        EventStreamOps.CountOp countOp = new EventStreamOps.CountOp(
            Categories.VIOLATION, "session_id", sessionId);
        Long count = (Long) violationRoute.adapter().execute(
            violationRoute.storeId(), violationRoute.kind(), countOp);
        assertNotNull(count, "CountOp dispatch must return a value");
        logger.info(() -> "dispatch ok: violations count for session " + sessionId + " = " + count);
    }

    private static UUID extractUuid(Object raw) {
        if (raw instanceof org.bson.types.Binary b) {
            byte[] d = b.getData();
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(d);
            return new UUID(buf.getLong(), buf.getLong());
        }
        if (raw instanceof UUID u) return u;
        throw new IllegalStateException("unexpected _id type: " + raw.getClass().getName());
    }

    private static void assumeReachable(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            Assumptions.assumeTrue(s.isConnected(), "mongo not reachable on " + host + ":" + port);
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "mongo not reachable on " + host + ":" + port + " (" + e.getMessage() + ")");
        }
    }
}
