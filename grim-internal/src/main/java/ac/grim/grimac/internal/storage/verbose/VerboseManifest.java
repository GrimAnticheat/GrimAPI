package ac.grim.grimac.internal.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Compact per-startup manifest for binary verbose payloads.
 *
 * <p>Violation rows store only {@code check_id + raw bytes}; this boot-scoped
 * manifest supplies the product flavor and per-check codec versions needed to
 * decode those bytes later. Checks absent from the map are legacy UTF-8 text.
 */
@ApiStatus.Internal
public final class VerboseManifest {

    public static final int FORMAT_VERSION = 1;

    public static final int FLAVOR_UNKNOWN = 0;
    public static final int FLAVOR_V2_PUBLIC = 1;
    public static final int FLAVOR_V3_PREMIUM = 2;

    private VerboseManifest() {}

    public static byte[] textOnly(int flavor) {
        return encode(flavor, Map.of());
    }

    public static byte[] encode(int flavor, @NotNull Map<Integer, Integer> checkCodecVersions) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4 + checkCodecVersions.size() * 3);
        out.write(FORMAT_VERSION);
        out.write(flavor & 0xFF);
        writeUVarInt(out, 0); // feature bits
        writeUVarInt(out, checkCodecVersions.size());

        int previousCheckId = 0;
        for (Map.Entry<Integer, Integer> entry : new TreeMap<>(checkCodecVersions).entrySet()) {
            int checkId = entry.getKey();
            if (checkId < previousCheckId) {
                throw new IllegalArgumentException("check ids must be non-decreasing");
            }
            int codecVersion = entry.getValue();
            if (codecVersion < 1) {
                throw new IllegalArgumentException("codec version must be positive");
            }
            writeUVarInt(out, checkId - previousCheckId);
            writeUVarInt(out, codecVersion);
            previousCheckId = checkId;
        }
        return out.toByteArray();
    }

    public static @NotNull Decoded decode(byte[] bytes) {
        Reader in = new Reader(bytes);
        int format = in.readU8();
        if (format != FORMAT_VERSION) {
            return new Decoded(format, FLAVOR_UNKNOWN, Map.of(), false);
        }
        int flavor = in.readU8();
        int featureBits = in.readUVarInt();
        if (featureBits != 0) {
            return new Decoded(format, flavor, Map.of(), false);
        }
        int count = in.readUVarInt();
        Map<Integer, Integer> codecs = new LinkedHashMap<>(Math.max(1, count));
        int checkId = 0;
        for (int i = 0; i < count; i++) {
            checkId += in.readUVarInt();
            codecs.put(checkId, in.readUVarInt());
        }
        return new Decoded(format, flavor, Map.copyOf(codecs), true);
    }

    private static void writeUVarInt(@NotNull ByteArrayOutputStream out, int value) {
        if (value < 0) throw new IllegalArgumentException("uvarint cannot encode negative value " + value);
        int v = value;
        while ((v & ~0x7F) != 0) {
            out.write((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        out.write(v);
    }

    public record Decoded(int formatVersion, int flavor, @NotNull Map<Integer, Integer> checkCodecVersions,
                          boolean supported) {
        public int codecVersionOrText(int checkId) {
            return checkCodecVersions.getOrDefault(checkId, 0);
        }
    }

    private static final class Reader {
        private final byte[] data;
        private int offset;

        private Reader(byte[] data) {
            this.data = data;
        }

        private int readU8() {
            if (offset >= data.length) throw new IllegalArgumentException("truncated verbose manifest");
            return data[offset++] & 0xFF;
        }

        private int readUVarInt() {
            int value = 0;
            int shift = 0;
            while (shift < 35) {
                int b = readU8();
                value |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) return value;
                shift += 7;
            }
            throw new IllegalArgumentException("verbose manifest varint is too long");
        }
    }
}
