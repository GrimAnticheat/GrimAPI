package ac.grim.grimac.internal.storage.history;

import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.config.TableNames;
import ac.grim.grimac.api.storage.history.SessionSummary;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.internal.storage.backend.SqlTestDatabases;
import ac.grim.grimac.internal.storage.backend.V2StoreFixture;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackendConfig;
import ac.grim.grimac.internal.storage.backend.redis.v2.RedisBackendV2;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import ac.grim.grimac.internal.storage.checks.InMemoryCheckCatalogPersistence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The session list's per-session counts come from the batched V2 operations whenever
 * violations have a V2 route, also when the host never installed a v2 violations
 * category (Grim does not), and they are right on every backend.
 */
@DisplayName("history session list counts violations per session")
class HistorySessionListIntegrationTest {

    @Test
    void sqlite(@TempDir Path dir) throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.sqlite(dir, "list.db")) {
            scenario(db.backend(), db.logs());
        }
    }

    @Test
    void postgres() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.postgres()) {
            scenario(db.backend(), db.logs());
        }
    }

    @Test
    void mysql() throws Exception {
        try (SqlTestDatabases.Target db = SqlTestDatabases.mysql()) {
            scenario(db.backend(), db.logs());
        }
    }

    @Test
    void mongo() throws Exception {
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        BackendV2 backend = SqlTestDatabases.mongo(logs);
        try {
            scenario(backend, logs);
        } finally {
            backend.close();
        }
    }

    @Test
    void redis() throws Exception {
        String host = System.getProperty("grim.test.redis.host", "localhost");
        int port = Integer.getInteger("grim.test.redis.port", 6379);
        SqlTestDatabases.assumeReachable(host, port);
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        RedisBackendConfig cfg = new RedisBackendConfig(
            host, port, 0, null, System.getProperty("grim.test.redis.password", "grim-test-redis"),
            "hlist" + Long.toHexString(System.nanoTime()) + ":", 2000, 256, false, TableNames.DEFAULTS);
        RedisBackendV2 backend = new RedisBackendV2(cfg);
        try {
            backend.init(new BackendContext() {
                @Override public Logger logger() { return logs.logger(); }
                @Override public Path dataDirectory() { return Path.of(System.getProperty("java.io.tmpdir")); }
                @Override public BackendConfig config() { return cfg; }
            });
            scenario(backend, logs);
        } finally {
            backend.close();
        }
    }

    private static void scenario(BackendV2 backend, SqlTestDatabases.Logs logs) throws Exception {
        long now = System.currentTimeMillis();
        UUID player = UUID.randomUUID();
        try (V2StoreFixture f = new V2StoreFixture(backend, "hlist", logs.logger())) {
            for (int s = 0; s < 5; s++) {
                UUID session = new UUID(9, s);
                f.session(session, player, now - (5 - s) * 3_600_000L, now - (5 - s) * 3_600_000L + 60_000L);
                // Session s has s * 3 violations over s distinct checks.
                for (int v = 0; v < s * 3; v++) {
                    f.violation(UUID.randomUUID(), session, player, 1 + v % s, now - (5 - s) * 3_600_000L + v, true);
                }
            }
            CheckRegistry checks = new CheckRegistry(new InMemoryCheckCatalogPersistence());
            checks.reload();
            HistoryServiceImpl history = new HistoryServiceImpl(f.store, checks, 15, 30_000L);
            Page<SessionSummary> page = history.listSessions(player, null, 15).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(5, page.items().size());
            for (SessionSummary summary : page.items()) {
                int s = (int) summary.sessionId().getLeastSignificantBits();
                assertEquals(s * 3L, summary.violationCount(), "violations of session " + s);
                assertEquals(s, summary.uniqueCheckCount(), "checks of session " + s);
            }
        }
    }
}
