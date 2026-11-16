package ac.grim.grimac.internal.storage.retention;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.RetentionRule;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.Deletes;
import ac.grim.grimac.internal.storage.backend.SqlTestDatabases;
import ac.grim.grimac.internal.storage.backend.V2StoreFixture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retention used to be parsed but never run: nothing scheduled the sweeper, and the
 * V2 lifecycle has no legacy backend for the session delete to fall back to. The
 * sweeper must now schedule itself, delete through the V2 adapters on every backend,
 * stop with the store, and only remove what the rules say.
 */
@DisplayName("Retention runs on its own and deletes through the V2 adapters")
class RetentionIntegrationTest {

    private static final long DAY = TimeUnit.DAYS.toMillis(1);
    private static final Map<Category<?>, RetentionRule> THIRTY_DAYS = Map.of(
        Categories.SESSION, new RetentionRule(true, 30),
        Categories.VIOLATION, new RetentionRule(true, 30),
        Categories.PLAYER_IDENTITY, new RetentionRule(true, 1));

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "retention.db")) {
            scenario(db.backend(), db.logs());
            batches(db.backend(), db.logs());
            batchLimit(db.backend(), db.logs());
        }
    }

    @Test
    void postgres() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.postgres()) {
            scenario(db.backend(), db.logs());
            batches(db.backend(), db.logs());
            batchLimit(db.backend(), db.logs());
        }
    }

    @Test
    void mysql() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.mysql()) {
            scenario(db.backend(), db.logs());
            batches(db.backend(), db.logs());
            batchLimit(db.backend(), db.logs());
        }
    }

    @Test
    void mongo() throws Exception {
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        BackendV2 backend = SqlTestDatabases.mongo(logs);
        try {
            scenario(backend, logs);
            batches(backend, logs);
            batchLimit(backend, logs);
        } finally {
            backend.close();
        }
    }

    /**
     * Existing configs already carry the bundled day values, so those alone must not delete:
     * the host's enable key, absent (the three-argument constructor) or false, keeps every row.
     */
    @Test
    void deletionIsOptIn(@TempDir Path dir) throws Exception {
        long now = System.currentTimeMillis();
        UUID player = UUID.randomUUID();
        UUID oldSession = UUID.randomUUID();
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "retention-optin.db");
             V2StoreFixture f = new V2StoreFixture(db.backend(), "ret_optin", db.logs().logger())) {
            f.session(oldSession, player, now - 400 * DAY, now - 400 * DAY);
            f.violation(UUID.randomUUID(), oldSession, player, 1, now - 400 * DAY, true);
            Map<Category<?>, RetentionRule> bundled = Map.of(
                Categories.SESSION, new RetentionRule(true, 90),
                Categories.VIOLATION, new RetentionRule(true, 365));

            db.logs().clear();
            RetentionSweeper absent = new RetentionSweeper(f.store, bundled, db.logs().logger());
            RetentionSweeper off = new RetentionSweeper(f.store, bundled, false, db.logs().logger(), 0L, 60_000L);
            assertFalse(absent.isScheduled());
            assertFalse(off.isScheduled());
            absent.sweepOnce();
            off.sweepOnce();
            assertTrue(sessionExists(f, oldSession), "nothing is deleted without the enable key");
            assertEquals(1, violations(f, oldSession));
            assertEquals(List.of(
                "[grim-datastore] History retention is configured but disabled; set database.retention.enabled: true"
                    + " to delete sessions older than 90 days and violations older than 365 days",
                "[grim-datastore] History retention is configured but disabled; set database.retention.enabled: true"
                    + " to delete sessions older than 90 days and violations older than 365 days"),
                db.logs().messages(Level.INFO));

            RetentionSweeper on = new RetentionSweeper(f.store, bundled, true, db.logs().logger(), 0L, 0L);
            on.sweepOnce();
            assertFalse(sessionExists(f, oldSession), "the enable key turns deletion on");
            assertEquals(0, violations(f, oldSession));
        }
    }

    @Test
    void disabledRulesScheduleNothing(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "retention-off.db");
             V2StoreFixture f = new V2StoreFixture(db.backend(), "ret_off", db.logs().logger())) {
            db.logs().clear();
            RetentionSweeper sweeper = new RetentionSweeper(f.store, Map.of(
                Categories.SESSION, new RetentionRule(false, 30),
                Categories.VIOLATION, RetentionRule.DISABLED,
                Categories.SETTING, new RetentionRule(true, 30)), true, db.logs().logger());
            assertFalse(sweeper.isScheduled());
            assertEquals(List.of(), db.logs().messages(Level.INFO));
            sweeper.close();
        }
    }

    private static void scenario(BackendV2 backend, SqlTestDatabases.Logs logs) throws Exception {
        long now = System.currentTimeMillis();
        UUID player = UUID.randomUUID();
        UUID oldSession = UUID.randomUUID();
        UUID oldButActive = UUID.randomUUID();
        UUID recentSession = UUID.randomUUID();
        try (V2StoreFixture f = new V2StoreFixture(backend, "ret", logs.logger())) {
            f.session(oldSession, player, now - 40 * DAY, now - 35 * DAY);
            f.session(oldButActive, player, now - 40 * DAY, now - DAY);
            f.session(recentSession, player, now - 2 * DAY, now - DAY);
            for (int i = 0; i < 3; i++) {
                f.violation(UUID.randomUUID(), oldSession, player, 1, now - 40 * DAY + i, false);
            }
            f.violation(UUID.randomUUID(), oldButActive, player, 1, now - 31 * DAY, false);
            f.violation(UUID.randomUUID(), oldButActive, player, 1, now - DAY, false);
            f.violation(UUID.randomUUID(), recentSession, player, 1, now - DAY, true);

            // Scheduled by construction; the first sweep runs at once here.
            RetentionSweeper sweeper = new RetentionSweeper(f.store, THIRTY_DAYS, true, logs.logger(), 0L, 60_000L);
            assertTrue(sweeper.isScheduled());
            long deadline = System.currentTimeMillis() + 30_000L;
            // Each category is a separate asynchronous delete. Wait for both categories;
            // the session disappearing does not mean the violation sweep has finished.
            while ((sessionExists(f, oldSession) || violations(f, oldSession) != 0
                    || violations(f, oldButActive) != 1) && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertFalse(sessionExists(f, oldSession), "a session older than 30 days is deleted");
            assertTrue(sessionExists(f, oldButActive), "a session active within 30 days is kept");
            assertTrue(sessionExists(f, recentSession));
            assertEquals(0, violations(f, oldSession), "violations older than 30 days are deleted");
            assertEquals(1, violations(f, oldButActive), "only the old violation of a live session goes");
            assertEquals(1, violations(f, recentSession));
            assertTrue(logs.messages(Level.INFO).stream().anyMatch(m -> m.startsWith("[grim-datastore] retention enabled")),
                logs.messages(Level.INFO).toString());

            // The session delete also works through DataStore.delete with no legacy backend.
            f.session(oldSession, player, now - 40 * DAY, now - 35 * DAY);
            f.store.delete(Categories.SESSION, Deletes.olderThan(30 * DAY)).toCompletableFuture().get(30, TimeUnit.SECONDS);
            assertFalse(sessionExists(f, oldSession));
            assertTrue(sessionExists(f, oldButActive));

            // Closing the store stops the schedule.
            f.store.flushAndClose(2_000L);
            assertFalse(sweeper.isScheduled());
            assertEquals(List.of(), logs.messages(Level.WARNING));
        }
    }

    /** A backlog larger than one batch is deleted in several. */
    private static void batches(BackendV2 backend, SqlTestDatabases.Logs logs) throws Exception {
        long now = System.currentTimeMillis();
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        int rows = RetentionSweeper.BATCH_SIZE * 2 + 7;
        try (V2StoreFixture f = new V2StoreFixture(backend, "retb", logs.logger())) {
            for (int i = 0; i < rows; i++) {
                f.violation(UUID.randomUUID(), session, player, i % 50, now - 60 * DAY + i, i % 500 == 499 || i == rows - 1);
            }
            logs.clear();
            new RetentionSweeper(f.store, Map.of(Categories.VIOLATION, new RetentionRule(true, 30)), true,
                logs.logger(), 0L, 0L).sweepOnce();
            assertEquals(0, violations(f, session));
            assertTrue(logs.messages(Level.INFO).contains(
                "[grim-datastore] retention deleted " + rows + " violation row(s) older than 30 days"),
                logs.messages(Level.INFO).toString());
        }
    }

    /** One batched delete removes at most its limit of old violations and leaves recent ones; 0 means all. */
    private static void batchLimit(BackendV2 backend, SqlTestDatabases.Logs logs) throws Exception {
        long now = System.currentTimeMillis();
        UUID player = UUID.randomUUID();
        UUID session = UUID.randomUUID();
        try (V2StoreFixture f = new V2StoreFixture(backend, "retl", logs.logger())) {
            for (int i = 0; i < 12; i++) f.violation(UUID.randomUUID(), session, player, 1, now - 60 * DAY + i * 1000L, false);
            f.violation(UUID.randomUUID(), session, player, 1, now - DAY, true);
            long cutoff = now - 30 * DAY;
            List<String> fields = List.of("occurred_at");
            assertEquals(5L, f.store.execute(new DeleteOlderThanOp(Categories.VIOLATION, fields, cutoff, 5))
                .toCompletableFuture().get(30, TimeUnit.SECONDS));
            assertEquals(8, violations(f, session));
            assertEquals(7L, f.store.execute(new DeleteOlderThanOp(Categories.VIOLATION, fields, cutoff, 0))
                .toCompletableFuture().get(30, TimeUnit.SECONDS));
            assertEquals(1, violations(f, session), "the recent violation stays");
        }
    }

    private static boolean sessionExists(V2StoreFixture f, UUID id) throws Exception {
        Optional<?> row = f.store.execute(new EntityOps.GetByIdOp<UUID, Object>(Categories.SESSION, id))
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
        return row.isPresent();
    }

    private static long violations(V2StoreFixture f, UUID session) throws Exception {
        return f.store.execute(new EventStreamOps.CountOp(Categories.VIOLATION, "session_id", session))
            .toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
}
