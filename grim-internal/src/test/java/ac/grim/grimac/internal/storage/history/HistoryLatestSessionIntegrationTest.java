package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.history.SessionDetail;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.SqlTestDatabases;
import ac.grim.grimac.internal.storage.backend.V2StoreFixture;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.InMemoryCheckCatalogPersistence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code /grim history <player> session latest} timed out on a database shared by
 * several servers: it reads one session's violations ({@code WHERE session_id = ?}),
 * and on MySQL grim_violations had no index on session_id, so every call scanned the
 * whole table. Seeds a shared database the size of a busy network (size with
 * {@code -Dgrim.test.history.violations}), then runs the command's calls with its
 * timeouts and checks the session read uses an index.
 */
@DisplayName("history session latest stays fast on a large shared database")
class HistoryLatestSessionIntegrationTest {

    private static final int VIOLATIONS = Integer.getInteger("grim.test.history.violations", 200_000);
    private static final int SESSIONS_PER_PLAYER = 10;
    private static final int VIOLATIONS_PER_SESSION = 50;
    private static final long BUDGET_MS = 3_000L;

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "history.db")) {
            scenario(db.backend(), db.logs(), db);
        }
    }

    @Test
    void postgres() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.postgres()) {
            scenario(db.backend(), db.logs(), db);
        }
    }

    @Test
    void mysql() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.mysql()) {
            scenario(db.backend(), db.logs(), db);
        }
    }

    @Test
    void mongo() throws Exception {
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        BackendV2 backend = SqlTestDatabases.mongo(logs);
        try {
            scenario(backend, logs, null);
        } finally {
            backend.close();
        }
    }

    private static void scenario(BackendV2 backend, SqlTestDatabases.Logs logs, SqlTestDatabases.Target sql)
            throws Exception {
        Random random = new Random(42);
        long now = System.currentTimeMillis();
        int sessions = Math.max(SESSIONS_PER_PLAYER, VIOLATIONS / VIOLATIONS_PER_SESSION);
        int players = Math.max(1, sessions / SESSIONS_PER_PLAYER);
        List<UUID> playerIds = new ArrayList<>();
        for (int i = 0; i < players; i++) playerIds.add(new UUID(random.nextLong(), random.nextLong()));

        try (V2StoreFixture f = new V2StoreFixture(backend, "hist", logs.logger())) {
            long seedStart = System.nanoTime();
            int written = 0;
            for (int s = 0; s < sessions; s++) {
                UUID player = playerIds.get(s % players);
                UUID session = new UUID(random.nextLong(), random.nextLong());
                // Sessions interleave across players and servers, newest last.
                long started = now - (long) (sessions - s) * 60_000L;
                f.session(session, player, started, started + 50_000L);
                for (int v = 0; v < VIOLATIONS_PER_SESSION; v++) {
                    written++;
                    f.violation(new UUID(random.nextLong(), random.nextLong()), session, player,
                        1 + random.nextInt(40), started + v * 1_000L, written % 256 == 0 || s == sessions - 1 && v == VIOLATIONS_PER_SESSION - 1);
                }
            }
            long seedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - seedStart);

            CheckRegistry checks = new CheckRegistry(new InMemoryCheckCatalogPersistence());
            checks.reload();
            HistoryServiceImpl history = new HistoryServiceImpl(f.store, checks, 15, 30_000L);
            UUID target = playerIds.get((sessions - 1) % players);

            long ms = latestSession(history, target);
            String label = sql == null ? "mongo" : sql.label();
            System.out.printf(Locale.ROOT, "[history] %s: %d violations, %d sessions seeded in %d ms;"
                + " session latest took %d ms%n", label, written, sessions, seedMs, ms);
            assertTrue(ms < BUDGET_MS, label + ": session latest took " + ms + " ms");

            if (sql != null) {
                assertUsesIndex(sql, f.violationsTable);
                if (sql.dialect().name().equals("mysql")) beforeTheFix(sql, f, history, target, label);
            }
        }
    }

    /** The command's calls, with its timeouts: count the sessions, then read the newest. */
    private static long latestSession(HistoryServiceImpl history, UUID player) throws Exception {
        long start = System.nanoTime();
        long total = history.countSessions(player).toCompletableFuture().get(5, TimeUnit.SECONDS);
        SessionDetail detail = history.getSessionDetailByOrdinal(player, (int) total)
            .toCompletableFuture().get(10, TimeUnit.SECONDS);
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals(SESSIONS_PER_PLAYER, total);
        assertNotNull(detail);
        assertEquals(VIOLATIONS_PER_SESSION, detail.violations().size());
        return ms;
    }

    private static void assertUsesIndex(SqlTestDatabases.Target sql, String table) throws Exception {
        SqlDialect d = sql.dialect();
        String select = "SELECT * FROM " + d.quoteIdentifier(table) + " WHERE " + d.quoteIdentifier("session_id")
            + " = " + switch (d.name()) {
                case "postgres" -> "'018f0000-0000-7000-8000-000000000001'";
                case "mysql" -> "UNHEX('018F0000000070008000000000000001')";
                default -> "X'018F0000000070008000000000000001'";
            } + " ORDER BY " + d.quoteIdentifier("occurred_at") + ", " + d.quoteIdentifier("id") + " LIMIT 10001";
        String explain = switch (d.name()) {
            case "sqlite" -> "EXPLAIN QUERY PLAN " + select;
            default -> "EXPLAIN " + select;
        };
        StringBuilder plan = new StringBuilder();
        try (Connection c = sql.open(); Statement s = c.createStatement()) {
            if (d.name().equals("postgres")) s.execute("ANALYZE " + d.quoteIdentifier(table));
            try (ResultSet rs = s.executeQuery(explain)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    for (int i = 1; i <= columns; i++) {
                        plan.append(rs.getMetaData().getColumnLabel(i)).append('=').append(rs.getString(i)).append(' ');
                    }
                    plan.append('\n');
                }
            }
        }
        String p = plan.toString();
        String index = "idx_" + table + "_session_id";
        assertTrue(p.contains(index), sql.label() + " does not use " + index + ":\n" + p);
        assertFalse(p.contains("Seq Scan") || p.contains("type=ALL"), sql.label() + " scans the table:\n" + p);
    }

    /** MySQL before the index fix: the same call without the default indexes, for the record. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void beforeTheFix(SqlTestDatabases.Target sql, V2StoreFixture f, HistoryServiceImpl history,
                                     UUID target, String label) throws Exception {
        SqlDialect d = sql.dialect();
        List<String> defaults = List.of("session_id", "player_uuid", "check_id", "ts");
        try (Connection c = sql.open(); Statement s = c.createStatement()) {
            for (String column : defaults) {
                s.executeUpdate("DROP INDEX " + d.quoteIdentifier("idx_" + f.violationsTable + "_" + column)
                    + " ON " + d.quoteIdentifier(f.violationsTable));
            }
        }
        long withoutIndexes = latestSessionUnbounded(history, target);
        long rebuildStart = System.nanoTime();
        KindAdapter adapter = (KindAdapter) sql.backend().adapterFor(V2BuiltinKinds.violations()).orElseThrow();
        adapter.ensureStore(StoreId.grim(f.violationsTable), V2BuiltinKinds.violations());
        long rebuildMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rebuildStart);
        System.out.printf(Locale.ROOT, "[history] %s without the default indexes: session latest took %d ms;"
            + " creating them at startup took %d ms%n", label, withoutIndexes, rebuildMs);
        assertUsesIndex(sql, f.violationsTable);
    }

    private static long latestSessionUnbounded(HistoryServiceImpl history, UUID player) throws Exception {
        long start = System.nanoTime();
        long total = history.countSessions(player).toCompletableFuture().get(5, TimeUnit.MINUTES);
        history.getSessionDetailByOrdinal(player, (int) total).toCompletableFuture().get(5, TimeUnit.MINUTES);
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }
}
