package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke test for the v2 Mongo adapters against a populated legacy
 * database. Targets the {@code grim_storage_test_19_4} test fixture
 * that the Phase A workflow built via a real paper-1.19.4 + Wurst
 * client run (see
 * {@code /tmp/mongo-phase-a-baseline.json}).
 *
 * <p>Verifies:
 * <ol>
 *   <li>{@link MongoBackendV2#init} brings the backend up against a
 *       collection set populated by the legacy {@code MongoBackend}.</li>
 *   <li>The v6→v7 migrations from each adapter's
 *       {@link Migration#apply} actually run end-to-end against the
 *       fixture data without throwing.</li>
 *   <li>v2 reads succeed against the migrated collections — exercises
 *       the codec's null-→sentinel substitution for legacy
 *       {@code closed_at:null} sessions (per Phase 5b.5.2) and the
 *       subtype-0 UUID round-trip.</li>
 * </ol>
 *
 * <p>Gated on {@code GRIM_V2_SMOKE_DB} system property — set the
 * environment variable to the database name (e.g.
 * {@code grim_storage_test_19_4}) to exercise; skipped otherwise so
 * the regular test suite stays self-contained.
 */
@DisplayName("v2 Mongo adapters against populated legacy fixture")
class V2MigrationSmokeTest {

    private static final String MONGO_CS = "mongodb://root:grim-test-mongo@localhost:27017/?authSource=admin";

    /** Collected failure messages across migrations + read steps. Aggregated to a single end-of-test assert. */
    private final List<String> failures = new java.util.ArrayList<>();

    @Test
    @DisplayName("init + migrate + read against legacy v6 fixture")
    void smoke() throws Exception {
        String dbName = System.getProperty("GRIM_V2_SMOKE_DB", System.getenv("GRIM_V2_SMOKE_DB"));
        Assumptions.assumeTrue(dbName != null && !dbName.isEmpty(),
            "set GRIM_V2_SMOKE_DB to the populated test database (e.g. grim_storage_test_19_4)");
        assumeReachable("localhost", 27017);

        Logger logger = Logger.getLogger("V2MigrationSmokeTest");

        // ---- Construct + init the v2 backend against the fixture ----
        MongoBackendConfig cfg = new MongoBackendConfig(
            MONGO_CS, dbName, 64, TableNames.DEFAULTS);
        MongoBackendV2 backend = new MongoBackendV2(cfg);
        backend.init(new BackendContext() {
            @Override public Logger logger() { return logger; }
            @Override public Path dataDirectory() { return Path.of("/tmp"); }
            @Override public BackendConfig config() { return cfg; }
        });

        try (MongoClient probeClient = MongoClients.create(MONGO_CS)) {
            MongoDatabase db = probeClient.getDatabase(dbName);
            long preSessionCount = db.getCollection("grim_sessions").countDocuments();
            long preViolationCount = db.getCollection("grim_violations").countDocuments();
            logger.info(() -> "fixture pre-migration: sessions=" + preSessionCount + " violations=" + preViolationCount);
            // Soft assertion: log the absence; don't fail. Some test
            // runs deliberately exercise the empty-collection path,
            // and we want the read-verification steps to run either way.
            if (preSessionCount == 0)   logger.warning(() -> "no legacy sessions in fixture — session read step will fail");
            if (preViolationCount == 0) logger.warning(() -> "no legacy violations in fixture — violations migration will no-op");

            // ---- Walk every builtin Kind, ensureStore, run migrations ----
            MongoMigrationContext mctx = new MongoMigrationContext(db, logger);

            // StoreId.name() IS the physical Mongo collection name. The
            // legacy MongoBackend uses table-prefixed names (grim_*), so
            // for migrating LEGACY data we point at those. The v2 adapter
            // operates on whatever physical name we pass. server_instances
            // and server_startups don't have legacy data so they use their
            // v2-native names.
            runMigrationsFor(backend, mctx, StoreId.grim("grim_violations"), V2BuiltinKinds.violations(), logger, failures);
            runMigrationsFor(backend, mctx, StoreId.grim("grim_sessions"),   V2BuiltinKinds.sessions(),   logger, failures);
            runMigrationsFor(backend, mctx, StoreId.grim("grim_players"),    V2BuiltinKinds.players(),    logger, failures);
            runMigrationsFor(backend, mctx, StoreId.grim("grim_checks"),     V2BuiltinKinds.checks(),     logger, failures);
            runMigrationsFor(backend, mctx, StoreId.grim("server_instances"), V2BuiltinKinds.instances(), logger, failures);
            runMigrationsFor(backend, mctx, StoreId.grim("server_startups"), V2BuiltinKinds.serverStartups(), logger, failures);

            // ---- Verify v2 reads work against the migrated fixture ----
            // Each verify* step is independent — collect findings
            // across all of them so a single test run surfaces every
            // gap, not just the first. tryStep records to `failures`
            // which gets asserted at end-of-method.
            tryStep("sessions read", logger, () -> verifySessionsReadable(backend, db, logger));
            tryStep("players read",  logger, () -> verifyPlayersReadable(backend, db, logger));
            tryStep("checks read",   logger, () -> verifyChecksReadable(backend, db, logger));

            // Schema markers updated post-migration.
            org.bson.Document meta = db.getCollection("grim_meta").find().first();
            assertNotNull(meta, "grim_meta should exist post-migration");
            logger.info(() -> "post-migration meta: " + meta.toJson());
        } finally {
            backend.close();
        }

        // End-of-test aggregation: if any migration or read step
        // failed, fail the JUnit test here with the collected
        // messages. Per Phase B review: a smoke test that silently
        // logs WARN on every failure isn't a test — it's a script.
        if (!failures.isEmpty()) {
            org.junit.jupiter.api.Assertions.fail(
                "v2 migration smoke detected " + failures.size() + " failure(s):\n  - "
                    + String.join("\n  - ", failures));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void runMigrationsFor(MongoBackendV2 backend, MongoMigrationContext ctx,
                                         StoreId id, DataKind kind, Logger logger,
                                         List<String> failures) {
        var adapterOpt = backend.adapterFor(kind);
        if (adapterOpt.isEmpty()) {
            failures.add("no adapter for kind " + kind.name());
            logger.warning(() -> "no adapter for kind " + kind.name() + " — skipping");
            return;
        }
        var adapter = adapterOpt.get();
        try {
            adapter.ensureStore(id, kind);
        } catch (Exception e) {
            failures.add("ensureStore failed for " + kind.name() + " on " + id + ": " + e.getMessage());
            logger.log(java.util.logging.Level.WARNING,
                "ensureStore failed for " + kind.name() + " on " + id, e);
        }
        List migs = adapter.migrations(kind);
        logger.info(() -> "running " + migs.size() + " migration(s) for " + kind.name());
        for (Object mo : migs) {
            Migration m = (Migration) mo;
            try {
                logger.info(() -> "  apply " + m.getClass().getSimpleName()
                    + " " + m.fromVersion() + " → " + m.toVersion());
                m.apply(ctx, id, kind);
                logger.info(() -> "  OK   " + m.getClass().getSimpleName());
            } catch (Exception e) {
                // Log + record + continue so subsequent migrations
                // and read steps still run. Aggregated failure raised
                // at end-of-test.
                failures.add("migration " + m.getClass().getSimpleName()
                    + " on " + id + ": " + e.getMessage());
                logger.log(java.util.logging.Level.WARNING,
                    "migration FAILED " + m.getClass().getSimpleName() + " on " + id, e);
            }
        }
    }

    private void tryStep(String label, Logger logger, ThrowingRunnable step) {
        try {
            step.run();
            logger.info(() -> "STEP OK: " + label);
        } catch (Throwable t) {
            failures.add(label + ": " + t.getClass().getSimpleName()
                + (t.getMessage() != null ? " — " + t.getMessage() : ""));
            logger.log(java.util.logging.Level.WARNING, "STEP FAIL: " + label, t);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void verifySessionsReadable(MongoBackendV2 backend, MongoDatabase db, Logger logger) throws Exception {
        Entity sessionsKind = V2BuiltinKinds.sessions();
        var adapter = backend.adapterFor(sessionsKind).orElseThrow();
        int read = 0, closed = 0, open = 0;
        for (org.bson.Document raw : db.getCollection("grim_sessions").find()) {
            Object idVal = raw.get("_id");
            // Round-trip the id through the V2 GetByIdOp path to
            // exercise codec.decode on a legacy-shaped session.
            UUID sessionId = extractUuid(idVal);
            EntityOps.GetByIdOp<UUID, SessionRecord> op = new EntityOps.GetByIdOp<>(
                Categories.SESSION, sessionId);
            Optional<SessionRecord> got = (Optional<SessionRecord>) adapter.execute(
                StoreId.grim("grim_sessions"), sessionsKind, op);
            assertTrue(got.isPresent(), "v2 must decode legacy session " + sessionId);
            SessionRecord s = got.get();
            read++;
            if (s.isClosed()) closed++; else open++;
        }
        final int rd = read, cl = closed, op2 = open;
        logger.info(() -> "sessions: read=" + rd + " closed=" + cl + " open=" + op2);
        assertTrue(read > 0, "should have read at least one session");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void verifyPlayersReadable(MongoBackendV2 backend, MongoDatabase db, Logger logger) throws Exception {
        Entity playersKind = V2BuiltinKinds.players();
        var adapter = backend.adapterFor(playersKind).orElseThrow();
        int read = 0;
        for (org.bson.Document raw : db.getCollection("grim_players").find()) {
            UUID uuid = extractUuid(raw.get("_id"));
            EntityOps.GetByIdOp<UUID, PlayerIdentity> op = new EntityOps.GetByIdOp<>(
                Categories.PLAYER_IDENTITY, uuid);
            Optional<PlayerIdentity> got = (Optional<PlayerIdentity>) adapter.execute(
                StoreId.grim("grim_players"), playersKind, op);
            assertTrue(got.isPresent(), "v2 must decode legacy player " + uuid);
            read++;
        }
        final int rd = read;
        logger.info(() -> "players: read=" + rd);
        assertTrue(read > 0, "should have read at least one player");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void verifyChecksReadable(MongoBackendV2 backend, MongoDatabase db, Logger logger) throws Exception {
        Entity checksKind = V2BuiltinKinds.checks();
        var adapter = backend.adapterFor(checksKind).orElseThrow();
        int read = 0;
        for (org.bson.Document raw : db.getCollection("grim_checks").find()) {
            String stableKey = raw.getString("_id");
            // No Categories.CHECK exists yet (Phase 1.4d work); the
            // adapter only consults the Category's identity for routing
            // it doesn't do here, so any Category placeholder works.
            EntityOps.GetByIdOp op = new EntityOps.GetByIdOp(
                Categories.SESSION, stableKey);
            Optional got = (Optional) adapter.execute(
                StoreId.grim("grim_checks"), checksKind, op);
            assertTrue(got.isPresent(), "v2 must decode legacy check " + stableKey);
            read++;
        }
        final int rd = read;
        logger.info(() -> "checks: read=" + rd);
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
