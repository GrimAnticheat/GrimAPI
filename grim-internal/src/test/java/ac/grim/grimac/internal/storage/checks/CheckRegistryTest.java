package ac.grim.grimac.internal.storage.checks;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour tests for {@link CheckRegistry} intern + collision-prefix.
 * Uses an in-memory {@link InMemoryPersistence} stand-in so the tests
 * run without SQLite and exercise the full intern path (insert, update,
 * collision resolution, concurrent access).
 */
class CheckRegistryTest {

    private InMemoryPersistence persistence;
    private CheckRegistry registry;

    @BeforeEach
    void setUp() {
        persistence = new InMemoryPersistence();
        // Static prefix keeps assertions readable; the DEFAULT_COLLISION_PREFIX
        // variants are exercised in their own test below.
        registry = new CheckRegistry(persistence, CheckRegistry.staticPrefix("V2/"));
    }

    @Test
    void internSameKeyTwiceIsIdempotent() {
        int first = registry.intern("grim.badpackets.duplicate_slot", "BadPacketsA", "Duplicate slot", "2.3.0");
        int second = registry.intern("grim.badpackets.duplicate_slot", "BadPacketsA", "Duplicate slot", "2.3.0");
        assertEquals(first, second, "same key should return the same id");
        assertEquals(1, registry.size(), "no extra rows");
        assertEquals(1, persistence.insertCount, "persistence insert fired exactly once");
    }

    @Test
    void internSameKeyWithNewDisplayUpdatesDisplay() {
        int id = registry.intern("grim.badpackets.respawn_alive", "BadPacketsM", "Respawn while alive", "2.3.0");

        // V3 boots and declares the same stable_key but a different display.
        int rebound = registry.intern("grim.badpackets.respawn_alive", "BadPacketsS", "Respawn while alive", "3.0.0");

        assertEquals(id, rebound, "same stable_key preserves id across a display rename");
        assertEquals("BadPacketsS", registry.displayFor(id).orElseThrow());
        assertEquals(1, persistence.insertCount, "no new row inserted — auto-unify updates in place");
        assertEquals(1, persistence.updateCount, "one display-update issued");
    }

    @Test
    void internNewKeyOnOccupiedDisplayPrefixesTheOlderRow() {
        // V2 checks in first: BadPacketsB means ignored_rotation.
        int v2Id = registry.intern("grim.badpackets.ignored_rotation", "BadPacketsB", "Ignored rotation", "2.3.0");
        assertEquals("BadPacketsB", registry.displayFor(v2Id).orElseThrow());

        // V3 boots and claims BadPacketsB for a different behaviour.
        int v3Id = registry.intern("grim.badpackets.invalid_steer", "BadPacketsB", "Invalid vehicle steer", "3.0.0");

        assertNotEquals(v2Id, v3Id, "different stable_key gets a new check_id");
        assertEquals("V2/BadPacketsB", registry.displayFor(v2Id).orElseThrow(),
                "older row's display got prefixed to free the letter");
        assertEquals("BadPacketsB", registry.displayFor(v3Id).orElseThrow(),
                "new row takes the now-free display");
        assertEquals(2, registry.size());
    }

    @Test
    void v3RestartIsIdempotentAfterCollisionPrefixLanded() {
        // Realistic lifecycle: V2 rows live in the DB already. V3 boots for
        // the first time and registers its BadPacketsB. Older row gets
        // prefixed; V3 plants the clean display. V3 then restarts — it
        // re-registers the same stable_key. No new rows, no re-prefix.
        int v2Id = registry.intern("grim.badpackets.ignored_rotation", "BadPacketsB", null, "2.3.0");
        int v3Id = registry.intern("grim.badpackets.invalid_steer", "BadPacketsB", "Invalid steer", "3.0.0");
        assertEquals("V2/BadPacketsB", registry.displayFor(v2Id).orElseThrow());
        assertEquals("BadPacketsB", registry.displayFor(v3Id).orElseThrow());
        assertEquals(2, registry.size());

        // V3 restarts (V2 is gone — it was upgraded away and no longer registers).
        int v3Again = registry.intern("grim.badpackets.invalid_steer", "BadPacketsB", "Invalid steer", "3.0.0");

        assertEquals(v3Id, v3Again, "same key returns same id");
        assertEquals(2, registry.size(), "no new rows");
        assertEquals("V2/BadPacketsB", registry.displayFor(v2Id).orElseThrow(),
                "older row's prefix persists across V3 restart");
        // Critically: no double-prefix. "V2/V2/BadPacketsB" would be a bug.
        assertAll(
                () -> assertTrue(registry.displayFor(v2Id).orElseThrow().startsWith("V2/")),
                () -> assertTrue(!registry.displayFor(v2Id).orElseThrow().startsWith("V2/V2/"),
                        "no double-prefix"));
    }

    @Test
    void newKeyWithPrefixedDisplayAlreadyInRegistryDoesNotCollide() {
        // V2 row gets prefixed by V3's BadPacketsB intern. Later, someone
        // introduces ANOTHER new check whose display happens to be
        // "V2/BadPacketsB" (pathological but possible if an operator
        // hand-named a check that way). That display is now occupied by
        // the prefixed V2 row; the new registration should collide and
        // prefix again — but our default prefix would produce
        // "V2/V2/BadPacketsB", which is fine, just ugly. This test
        // confirms no crash and no lost data.
        int v2Id = registry.intern("grim.bp.old", "BadPacketsB", null, "2.0.0");
        registry.intern("grim.bp.new", "BadPacketsB", null, "3.0.0");
        assertEquals("V2/BadPacketsB", registry.displayFor(v2Id).orElseThrow());

        int pathId = registry.intern("grim.weird.one", "V2/BadPacketsB", null, "2.0.0");

        assertEquals(3, registry.size(), "new row inserted, no merging of unrelated keys");
        // One of the two "V2/BadPacketsB" rows (the older one) gets prefixed again.
        assertEquals("V2/V2/BadPacketsB", registry.displayFor(v2Id).orElseThrow(),
                "original V2 row gets prefixed again since its display is now colliding");
        assertEquals("V2/BadPacketsB", registry.displayFor(pathId).orElseThrow(),
                "pathological new entry lands with its declared display");
    }

    @Test
    void concurrentInternForSameKeyReturnsSameId() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return registry.intern("grim.combat.reach", "Reach", "Reach exceeds bedrock envelope", "3.0.0");
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS), "threads ready");
            go.countDown();

            int expected = futures.get(0).get(5, TimeUnit.SECONDS);
            assertNotNull(expected);
            for (Future<Integer> f : futures) {
                assertEquals(expected, f.get(5, TimeUnit.SECONDS), "all threads converge on one id");
            }
            assertEquals(1, registry.size());
            assertEquals(1, persistence.insertCount, "exactly one insert across all threads");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void defaultCollisionPrefixReadsIntroducedMajorVersion() {
        CheckRegistry versioned = new CheckRegistry(persistence, CheckRegistry.DEFAULT_COLLISION_PREFIX);
        int v2Id = versioned.intern("grim.badpackets.ignored_rotation", "BadPacketsB", null, "2.3.61");
        versioned.intern("grim.badpackets.invalid_steer", "BadPacketsB", null, "3.0.0");
        assertEquals("V2/BadPacketsB", versioned.displayFor(v2Id).orElseThrow(),
                "default prefix reads major from introduced_version");
    }

    @Test
    void defaultCollisionPrefixFallsBackWhenVersionIsMissing() {
        CheckRegistry versioned = new CheckRegistry(persistence, CheckRegistry.DEFAULT_COLLISION_PREFIX);
        int oldId = versioned.intern("grim.something.old", "FooA", null, null);
        versioned.intern("grim.something.new", "FooA", null, "3.0.0");
        assertEquals("legacy/FooA", versioned.displayFor(oldId).orElseThrow(),
                "null introduced_version degrades to the 'legacy/' fallback");
    }

    @Test
    void defaultCollisionPrefixHandlesMessyVersionStrings() {
        CheckRegistry versioned = new CheckRegistry(persistence, CheckRegistry.DEFAULT_COLLISION_PREFIX);
        int oldId1 = versioned.intern("a", "X", null, "v2.3");
        versioned.intern("b", "X", null, "3.0");
        assertEquals("V2/X", versioned.displayFor(oldId1).orElseThrow());

        int oldId2 = versioned.intern("c", "Y", null, "Grim-3.0");
        versioned.intern("d", "Y", null, "4.0");
        assertEquals("V3/Y", versioned.displayFor(oldId2).orElseThrow(),
                "prefix strips non-digit characters and picks the leading number");

        int oldId3 = versioned.intern("e", "Z", null, "???");
        versioned.intern("f", "Z", null, "2.0");
        assertEquals("legacy/Z", versioned.displayFor(oldId3).orElseThrow(),
                "unparseable version degrades to legacy/");
    }

    // ---- in-memory persistence ----

    private static final class InMemoryPersistence implements CheckRegistry.CheckPersistence {
        private final Map<Integer, CheckRegistry.CheckRow> rows = new ConcurrentHashMap<>();
        private final Map<String, Integer> byKey = new ConcurrentHashMap<>();
        private final AtomicInteger nextId = new AtomicInteger(1);
        int insertCount = 0;
        int updateCount = 0;

        @Override
        public Iterable<CheckRegistry.CheckRow> loadAll() {
            return new ArrayList<>(rows.values());
        }

        @Override
        public synchronized int insert(String stableKey,
                                       @Nullable String display,
                                       @Nullable String description,
                                       @Nullable String introducedVersion,
                                       long introducedAt) {
            Integer existing = byKey.get(stableKey);
            if (existing != null) return existing;
            int id = nextId.getAndIncrement();
            rows.put(id, new CheckRegistry.CheckRow(
                    id, stableKey, display, description, introducedVersion, introducedAt));
            byKey.put(stableKey, id);
            insertCount++;
            return id;
        }

        @Override
        public synchronized void updateDisplayAndDescription(int checkId,
                                                             @Nullable String display,
                                                             @Nullable String description) {
            CheckRegistry.CheckRow row = rows.get(checkId);
            if (row == null) return;
            rows.put(checkId, new CheckRegistry.CheckRow(
                    row.checkId(), row.stableKey(), display, description,
                    row.introducedVersion(), row.introducedAt()));
            updateCount++;
        }
    }
}
