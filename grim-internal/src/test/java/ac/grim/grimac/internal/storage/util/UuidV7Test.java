package ac.grim.grimac.internal.storage.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UuidV7Test {

    @Test
    void deterministicMigrationIdsPreserveSameTimestampSequenceOrder() {
        long ts = 1_700_000_000_000L;

        UUID first = UuidV7.fromTimestampMs(ts, 41L);
        UUID second = UuidV7.fromTimestampMs(ts, 42L);

        assertTrue(first.compareTo(second) < 0);
        assertEquals(first, UuidV7.fromTimestampMs(ts, 41L));
        assertEquals(ts, timestampMs(first));
    }

    @Test
    void timestampClampDoesNotWrapFutureValues() {
        long max = (1L << 48) - 1L;

        assertEquals(0L, timestampMs(UuidV7.fromTimestampMs(-1L)));
        assertEquals(max, timestampMs(UuidV7.fromTimestampMs(max + 1L)));
    }

    @Test
    void sequenceOverflowAdvancesLogicalTimestamp() throws Exception {
        // Force the internal state to (fixedMs, seq=0x3FFE) so the next two
        // calls hit (a) the last slot of the current ms and (b) the overflow
        // path that advances the logical timestamp.
        Field stateField = UuidV7.class.getDeclaredField("STATE");
        stateField.setAccessible(true);
        AtomicLong state = (AtomicLong) stateField.get(null);

        long savedState = state.get();
        try {
            long fixedMs = System.currentTimeMillis() + 10_000L;
            state.set((fixedMs << 14) | 0x3FFE);

            UUID lastInMs = UuidV7.next();
            UUID logicalNextMs = UuidV7.next();

            assertTrue(lastInMs.compareTo(logicalNextMs) < 0);
            assertEquals(timestampMs(lastInMs) + 1, timestampMs(logicalNextMs));
        } finally {
            state.set(savedState);
        }
    }

    private static long timestampMs(UUID uuid) {
        return (uuid.getMostSignificantBits() >>> 16) & ((1L << 48) - 1L);
    }
}
