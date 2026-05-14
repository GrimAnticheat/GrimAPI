package ac.grim.grimac.internal.storage.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UuidV7Test {

    @Test
    void sequenceOverflowAdvancesLogicalTimestamp() throws Exception {
        Field lastMsField = UuidV7.class.getDeclaredField("lastMs");
        Field seqField = UuidV7.class.getDeclaredField("seq");
        lastMsField.setAccessible(true);
        seqField.setAccessible(true);

        try {
            long fixedMs = System.currentTimeMillis() + 10_000L;
            lastMsField.setLong(null, fixedMs);
            seqField.setInt(null, 0x3FFE);

            UUID lastInMs = UuidV7.next();
            UUID logicalNextMs = UuidV7.next();

            assertTrue(lastInMs.compareTo(logicalNextMs) < 0);
            assertEquals(timestampMs(lastInMs) + 1, timestampMs(logicalNextMs));
        } finally {
            lastMsField.setLong(null, -1L);
            seqField.setInt(null, 0);
        }
    }

    private static long timestampMs(UUID uuid) {
        return (uuid.getMostSignificantBits() >>> 16) & ((1L << 48) - 1L);
    }
}
