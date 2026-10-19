package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisServerOwnershipAdapterTest {
    private static final StoreId STORE = StoreId.grim("ownership");
    private static Process redis;
    private static int port;
    private final UUID persistent = UUID.randomUUID();
    private final String prefix = "ownership-test:" + UUID.randomUUID() + ":";

    @BeforeAll
    static void startRedis(@TempDir Path directory) throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        try {
            redis = new ProcessBuilder("redis-server", "--bind", "127.0.0.1", "--port", Integer.toString(port),
                    "--save", "", "--appendonly", "no", "--dir", directory.toString())
                    .redirectErrorStream(true).redirectOutput(directory.resolve("redis.log").toFile()).start();
        } catch (java.io.IOException unavailable) {
            assumeTrue(false, "redis-server is required for the real Redis ownership tests");
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && redis.isAlive()) {
            try (Jedis jedis = new Jedis("127.0.0.1", port)) {
                if ("PONG".equals(jedis.ping())) return;
            } catch (RuntimeException starting) { Thread.sleep(20); }
        }
        fail("isolated Redis process did not start");
    }

    @AfterAll
    static void stopRedis() throws Exception {
        if (redis != null) {
            redis.destroy();
            if (!redis.waitFor(5, TimeUnit.SECONDS)) redis.destroyForcibly();
        }
    }

    @Test
    void atomicClaimReturnsAnInterveningOwnersFullSnapshot() throws Exception {
        UUID oldOwner = UUID.randomUUID();
        seed(oldOwner, UUID.randomUUID(), 1L);
        UUID intervening = UUID.randomUUID();
        UUID token = UUID.randomUUID();
        RedisServerOwnershipAdapter adapter = adapter(j -> seed(intervening, token, 2L), null);

        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());

        assertTrue(result.claimed());
        assertEquals(intervening, result.previousOwner().ownerStartupId());
        assertEquals(token, result.previousOwner().fence());
        assertEquals(2L, result.previousOwner().leaseExpiresAtEpochMs());
        assertEquals("test", result.previousOwner().serverName());
        assertEquals(20_000, result.leaseExpiresAtEpochMs() - result.dbNowEpochMs());
    }

    @Test
    void rejectionCannotBecomeSuccessWhenTheRowDisappearsAfterTheScript() throws Exception {
        UUID owner = UUID.randomUUID();
        seed(owner, UUID.randomUUID(), System.currentTimeMillis() + 60_000);
        RedisServerOwnershipAdapter adapter = adapter(null, j -> {
            try (Jedis connection = new Jedis("127.0.0.1", port)) { connection.del(key()); }
        });
        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        assertFalse(result.claimed());
        assertEquals(owner, result.currentOwner().ownerStartupId());
    }

    @Test
    void newClaimRenewCloseAndReclaimUseTheSameContract() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        UUID startup = UUID.randomUUID();
        UUID fence = UUID.randomUUID();
        OwnershipClaimResult first = adapter.claimOwnership(STORE, persistent, startup, fence, 20_000, metadata());
        assertTrue(first.claimed());
        assertNull(first.previousOwner());
        assertTrue(adapter.renewOwnership(STORE, persistent, startup, fence, 20_000).renewed());
        assertTrue(adapter.closeOwnership(STORE, persistent, startup, fence, "shutdown"));
        OwnershipClaimResult next = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        assertTrue(next.claimed());
        assertEquals(startup, next.previousOwner().ownerStartupId());
        assertEquals("shutdown", next.previousOwner().closeReason());
        assertFalse(adapter.renewOwnership(STORE, persistent, startup, fence, 20_000).renewed());
    }

    @Test
    void expiredLeaseRejectsRenewalAndCanBeReclaimedWithFreshIdentity() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        UUID oldStartup = UUID.randomUUID();
        UUID oldFence = UUID.randomUUID();
        // A persisted lease whose deadline passed while the DB was unavailable.
        seed(oldStartup, oldFence, 1L);
        assertFalse(adapter.renewOwnership(STORE, persistent, oldStartup, oldFence, 20_000).renewed());
        UUID newStartup = UUID.randomUUID();
        UUID newFence = UUID.randomUUID();
        OwnershipClaimResult claim = adapter.claimOwnership(STORE, persistent, newStartup, newFence, 20_000, metadata());
        assertTrue(claim.claimed());
        assertEquals(oldStartup, claim.previousOwner().ownerStartupId());
        assertEquals(oldFence, claim.previousOwner().fence());
        assertTrue(adapter.renewOwnership(STORE, persistent, newStartup, newFence, 20_000).renewed());
        assertFalse(adapter.closeOwnership(STORE, persistent, oldStartup, oldFence, "stale"));
    }

    @Test
    void concurrentClaimsAdmitExactlyOneOwner() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<OwnershipClaimResult> claim = () -> {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS));
            return adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        };
        try {
            Future<OwnershipClaimResult> first = executor.submit(claim);
            Future<OwnershipClaimResult> second = executor.submit(claim);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            OwnershipClaimResult a = first.get(5, TimeUnit.SECONDS);
            OwnershipClaimResult b = second.get(5, TimeUnit.SECONDS);
            assertNotEquals(a.claimed(), b.claimed());
            OwnershipClaimResult winner = a.claimed() ? a : b;
            OwnershipClaimResult loser = a.claimed() ? b : a;
            assertEquals(winner.startupId(), loser.currentOwner().ownerStartupId());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private RedisServerOwnershipAdapter adapter(Consumer<Jedis> before, Consumer<Jedis> after) {
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenAnswer(call -> {
            Jedis connection = spy(new Jedis("127.0.0.1", port));
            doAnswer(eval -> {
                if (before != null) before.accept(connection);
                Object result = eval.callRealMethod();
                if (after != null) after.accept(connection);
                return result;
            }).when(connection).eval(anyString(), anyList(), anyList());
            return connection;
        });
        return new RedisServerOwnershipAdapter(pool, prefix, Logger.getLogger(getClass().getName()));
    }

    private void seed(UUID owner, UUID fence, long expires) {
        try (Jedis jedis = new Jedis("127.0.0.1", port)) {
            jedis.hset(key(), Map.of("persistent_id", persistent.toString(), "owner_startup_id", owner.toString(),
                    "fence", fence.toString(), "lease_expires_at_ms", Long.toString(expires),
                    "last_renewed_at_ms", "1", "closed_at_ms", "0", "server_name", "test"));
        }
    }

    private String key() { return prefix + STORE.name() + ":" + persistent; }
    private static ServerOwnershipMetadata metadata() { return new ServerOwnershipMetadata("test", null, null, null); }
}
