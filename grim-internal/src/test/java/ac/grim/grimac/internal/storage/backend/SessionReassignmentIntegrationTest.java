package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.mongo.v2.MongoEntityAdapter;
import ac.grim.grimac.internal.storage.backend.redis.v2.RedisEntityAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.SqlEntityAdapter;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Jedis;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class SessionReassignmentIntegrationTest {
    private static final Logger LOG = Logger.getLogger(SessionReassignmentIntegrationTest.class.getName());

    @Test
    void sqlitePreservesOneSessionAcrossCrashClosureAndHandoff(@TempDir Path directory) throws Exception {
        SQLiteDataSource ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:" + directory.resolve("sessions.sqlite"));
        exercise(new SqlEntityAdapter(ds, new SqliteDialect(), LOG));
    }

    @Test
    void redisPreservesOneSessionAndMovesItsIndexes(@TempDir Path directory) throws Exception {
        try (Server server = start(directory, "redis-server", "--save", "", "--appendonly", "no", "--bind", "127.0.0.1" );
             JedisPool pool = new JedisPool("127.0.0.1", server.port)) {
            exercise(new RedisEntityAdapter(pool, "session-test:", LOG));
        }
    }

    @Test
    void mongoPreservesOneSessionAcrossCrashClosureAndHandoff(@TempDir Path directory) throws Exception {
        String binary = System.getenv().getOrDefault("MONGOD_BINARY", "mongod");
        Path db = Files.createDirectories(directory.resolve("db"));
        try (Server server = start(directory, binary, "--dbpath", db.toString(), "--bind_ip", "127.0.0.1", "--nounixsocket", "--quiet");
             var client = MongoClients.create("mongodb://127.0.0.1:" + server.port + "/?serverSelectionTimeoutMS=10000")) {
            exercise(new MongoEntityAdapter(client.getDatabase("session_test"), LOG));
        }
    }

    @Test
    void redisStaleCleanupScanCannotCloseAMovedSession(@TempDir Path directory) throws Exception {
        staleRedisCleanup(directory, true);
    }

    @Test
    void redisCleanupRetriesWhenHandoffChangesTheWatchedRow(@TempDir Path directory) throws Exception {
        staleRedisCleanup(directory, false);
    }

    private void staleRedisCleanup(Path directory, boolean transferAfterScan) throws Exception {
        try (Server server = start(directory, "redis-server", "--save", "", "--appendonly", "no", "--bind", "127.0.0.1");
             JedisPool realPool = new JedisPool("127.0.0.1", server.port)) {
            String prefix = "race:";
            var adapter = new RedisEntityAdapter(realPool, prefix, LOG);
            StoreId store = StoreId.grim("sessions");
            var kind = V2BuiltinKinds.sessions();
            UUID session = UUID.randomUUID();
            UUID oldStartup = UUID.randomUUID();
            UUID nextStartup = UUID.randomUUID();
            adapter.writeHandler(store, kind, Categories.SESSION).onEvent(new SessionEvent()
                    .sessionId(session).playerUuid(UUID.randomUUID()).startupId(oldStartup)
                    .startedEpochMs(100).lastActivityEpochMs(200).clientVersion(765), 0, true);
            AtomicBoolean transferred = new AtomicBoolean();
            JedisPool racingPool = mock(JedisPool.class);
            when(racingPool.getResource()).thenAnswer(acquire -> {
                Jedis connection = spy(new Jedis("127.0.0.1", server.port));
                org.mockito.stubbing.Answer<Object> race = call -> {
                    Object result = call.callRealMethod();
                    if (transferred.compareAndSet(false, true)) {
                        assertEquals(1L, (long) adapter.execute(store, kind, new EntityOps.ReassignSessionOp(
                                Categories.SESSION, session, Set.of(oldStartup), nextStartup)));
                    }
                    return result;
                };
                if (transferAfterScan) doAnswer(race).when(connection).zrange(anyString(), anyLong(), anyLong());
                else doAnswer(race).when(connection).hgetAll(any(byte[].class));
                return connection;
            });
            var sweeper = new RedisEntityAdapter(racingPool, prefix, LOG);
            assertEquals(0L, (long) sweeper.execute(store, kind, new EntityOps.SetIfSentinelOp(
                    Categories.SESSION, "by_startup_open", oldStartup, "closed_at", 0L, null, "last_activity")));
            assertTrue(transferred.get());
            SessionRecord row = read(adapter, store, kind, session);
            assertEquals(nextStartup, row.startupId());
            assertFalse(row.isClosed());
        }
    }

    private void exercise(KindAdapter<Entity<?, ?, ?>> adapter) throws Exception {
        StoreId store = StoreId.grim("sessions");
        var kind = V2BuiltinKinds.sessions();
        adapter.ensureStore(store, kind);
        var writer = adapter.writeHandler(store, kind, Categories.SESSION);
        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        UUID oldStartup = UUID.randomUUID();
        UUID newStartup = UUID.randomUUID();
        SessionEvent event = new SessionEvent().sessionId(session).playerUuid(player).startupId(oldStartup)
                .startedEpochMs(100L).lastActivityEpochMs(200L).clientBrand("fabric").clientVersion(765);
        writer.onEvent(event, 0, true);
        var oldSweep = new EntityOps.SetIfSentinelOp(Categories.SESSION, "by_startup_open", oldStartup,
                "closed_at", 0L, null, "last_activity");
        assertEquals(1L, (long) adapter.execute(store, kind, oldSweep));
        assertTrue(read(adapter, store, kind, session).isClosed());

        var resume = new EntityOps.ReassignSessionOp(Categories.SESSION, session, Set.of(oldStartup), newStartup);
        assertEquals(1L, (long) adapter.execute(store, kind, resume));
        SessionRecord restored = read(adapter, store, kind, session);
        assertEquals(session, restored.sessionId());
        assertEquals(100L, restored.startedEpochMs());
        assertEquals("fabric", restored.clientBrand());
        assertEquals(765, restored.clientVersion());
        assertEquals(newStartup, restored.startupId());
        assertFalse(restored.isClosed());
        assertEquals(0L, (long) adapter.execute(store, kind, oldSweep), "stale cleanup must not close the moved session");
        Page<SessionRecord> oldIndex = adapter.execute(store, kind,
                new EntityOps.FindByIndexOp<>(Categories.SESSION, "by_startup_open", oldStartup, null, 10));
        Page<SessionRecord> newIndex = adapter.execute(store, kind,
                new EntityOps.FindByIndexOp<>(Categories.SESSION, "by_startup_open", newStartup, null, 10));
        assertTrue(oldIndex.items().isEmpty());
        assertEquals(1, newIndex.items().size());

        // Even a delayed heartbeat from before the handoff cannot revert ownership.
        writer.onEvent(event, 1, true);
        assertEquals(newStartup, read(adapter, store, kind, session).startupId());
        event.startupId(newStartup).closedAtEpochMs(300L);
        writer.onEvent(event, 2, true);
        assertEquals(0L, (long) adapter.execute(store, kind, resume), "retry must not reopen a real quit");
        event.closedAtEpochMs(0L);
        writer.onEvent(event, 3, true);
        assertEquals(300L, read(adapter, store, kind, session).closedAtEpochMs(), "heartbeats must preserve closure");
        assertEquals(0L, (long) adapter.execute(store, kind,
                new EntityOps.ReassignSessionOp(Categories.SESSION, session, Set.of(UUID.randomUUID()), UUID.randomUUID())));
        assertEquals(newStartup, read(adapter, store, kind, session).startupId());
    }

    private static SessionRecord read(KindAdapter<Entity<?, ?, ?>> adapter, StoreId store, Entity<?, ?, ?> kind, UUID id) throws Exception {
        Optional<SessionRecord> row = adapter.execute(store, kind, new EntityOps.GetByIdOp<>(Categories.SESSION, id));
        return row.orElseThrow();
    }

    private static Server start(Path directory, String binary, String... args) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        List<String> command = new ArrayList<>(List.of(binary));
        command.addAll(List.of(args));
        command.addAll(List.of("--port", Integer.toString(port)));
        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(directory.resolve("server.log").toFile()).start();
        } catch (java.io.IOException unavailable) {
            assumeTrue(false, binary + " is required for this integration test");
            throw unavailable;
        }
        Server server = new Server(process, port);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline && process.isAlive()) {
            try (Socket socket = new Socket("127.0.0.1", port)) { return server; }
            catch (java.io.IOException waiting) { Thread.sleep(20); }
        }
        server.close();
        fail("isolated " + binary + " failed to start: " + Files.readString(directory.resolve("server.log")));
        return server;
    }

    private record Server(Process process, int port) implements AutoCloseable {
        public void close() throws Exception {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
    }
}
