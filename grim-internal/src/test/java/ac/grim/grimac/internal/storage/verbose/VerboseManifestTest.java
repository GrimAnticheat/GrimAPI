package ac.grim.grimac.internal.storage.verbose;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerboseManifestTest {

    @Test
    void textOnlyManifestMarksAllChecksAsLegacyText() {
        VerboseManifest.Decoded decoded = VerboseManifest.decode(
                VerboseManifest.textOnly(VerboseManifest.FLAVOR_V3_PREMIUM));

        assertTrue(decoded.supported());
        assertEquals(VerboseManifest.FORMAT_VERSION, decoded.formatVersion());
        assertEquals(VerboseManifest.FLAVOR_V3_PREMIUM, decoded.flavor());
        assertEquals(0, decoded.codecVersionOrText(42));
    }

    @Test
    void encodeSortsCheckIdsForStableStartupPayloads() {
        Map<Integer, Integer> unsorted = new LinkedHashMap<>();
        unsorted.put(130, 4);
        unsorted.put(3, 1);
        unsorted.put(9, 2);

        byte[] encoded = VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC, unsorted);
        byte[] expected = VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC,
                Map.of(3, 1, 9, 2, 130, 4));

        assertArrayEquals(expected, encoded);

        VerboseManifest.Decoded decoded = VerboseManifest.decode(encoded);
        assertTrue(decoded.supported());
        assertEquals(VerboseManifest.FLAVOR_V2_PUBLIC, decoded.flavor());
        assertEquals(1, decoded.codecVersionOrText(3));
        assertEquals(2, decoded.codecVersionOrText(9));
        assertEquals(4, decoded.codecVersionOrText(130));
        assertEquals(0, decoded.codecVersionOrText(131));
    }

    @Test
    void encodeRejectsTextVersionEntries() {
        assertThrows(IllegalArgumentException.class,
                () -> VerboseManifest.encode(VerboseManifest.FLAVOR_V2_PUBLIC, Map.of(42, 0)));
    }

    @Test
    void unknownManifestVersionIsUnsupportedButNonThrowing() {
        VerboseManifest.Decoded decoded = VerboseManifest.decode(new byte[] { 99 });

        assertFalse(decoded.supported());
        assertEquals(99, decoded.formatVersion());
        assertEquals(VerboseManifest.FLAVOR_UNKNOWN, decoded.flavor());
        assertEquals(0, decoded.codecVersionOrText(1));
    }
}
