package ac.grim.grimac.internal.storage.checks;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CheckRegistryTest {

    @Test
    void internAssignsMonotonicIdsAndIsIdempotent() {
        InMemoryPersistence p = new InMemoryPersistence();
        CheckRegistry reg = new CheckRegistry(p);
        int reachId = reg.intern("combat.reach", "Reach");
        int timerId = reg.intern("movement.timer", "Timer");
        int reachAgain = reg.intern("combat.reach", "Reach");
        assertEquals(reachId, reachAgain);
        assertNotEquals(reachId, timerId);
        assertEquals(2, reg.size());
    }

    @Test
    void internRenamesDisplayButKeepsId() {
        InMemoryPersistence p = new InMemoryPersistence();
        CheckRegistry reg = new CheckRegistry(p);
        int id = reg.intern("combat.reach", "Reach");
        int sameId = reg.intern("combat.reach", "NewReach");
        assertEquals(id, sameId);
        assertEquals("NewReach", reg.displayFor(id).orElseThrow());
    }

    @Test
    void reloadRestoresAllRows() {
        InMemoryPersistence p = new InMemoryPersistence();
        CheckRegistry a = new CheckRegistry(p);
        int r = a.intern("combat.reach", "Reach");
        int t = a.intern("movement.timer", "Timer");

        CheckRegistry b = new CheckRegistry(p);
        b.reload();
        assertEquals(r, b.getId("combat.reach").orElseThrow());
        assertEquals(t, b.getId("movement.timer").orElseThrow());
    }

    @Test
    void stableKeyMappingKnowsCommonLegacyNames() {
        assertEquals("combat.reach", StableKeyMapping.stableKeyFor("Reach").orElseThrow());
        assertEquals("combat.reach", StableKeyMapping.stableKeyFor("REACH").orElseThrow(),
                "mapping is case-insensitive");
        assertEquals("movement.timer", StableKeyMapping.stableKeyFor("Timer").orElseThrow());
        assertTrue(StableKeyMapping.stableKeyFor("Unknown_new_Check").isEmpty());
        assertEquals("legacy:unknown_new_check", StableKeyMapping.legacyFallback("Unknown_new_Check"));
    }

    private static final class InMemoryPersistence implements CheckRegistry.CheckPersistence {
        private final Map<String, CheckRegistry.CheckRow> byKey = new HashMap<>();
        private final Map<Integer, CheckRegistry.CheckRow> byId = new HashMap<>();
        private int nextId = 1;

        @Override public Iterable<CheckRegistry.CheckRow> loadAll() {
            return new ArrayList<>(byKey.values());
        }
        @Override public int insert(String stableKey, String display) {
            int id = nextId++;
            CheckRegistry.CheckRow row = new CheckRegistry.CheckRow(id, stableKey, display);
            byKey.put(stableKey, row);
            byId.put(id, row);
            return id;
        }
        @Override public void updateDisplay(int checkId, String display) {
            CheckRegistry.CheckRow prev = byId.get(checkId);
            if (prev == null) return;
            CheckRegistry.CheckRow row = new CheckRegistry.CheckRow(checkId, prev.stableKey(), display);
            byKey.put(prev.stableKey(), row);
            byId.put(checkId, row);
        }
    }
}
