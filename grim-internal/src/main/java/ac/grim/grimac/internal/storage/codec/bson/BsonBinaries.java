package ac.grim.grimac.internal.storage.codec.bson;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonWriter;
import org.bson.types.Binary;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * UUID and float[] helpers for the BSON codec layer.
 * <p>
 * <b>UUID subtype policy</b>: new writes always use
 * {@link BsonBinarySubType#UUID_STANDARD} (subtype 4) — interoperable with
 * any Mongo client. Reads accept any 16-byte binary regardless of subtype:
 * the legacy v6 backend wrote {@code new BsonBinary(byte[])} which defaults
 * to generic subtype 0 ({@link BsonBinarySubType#BINARY}); accepting both
 * keeps reads working through the v6 → v7 migration window. The migration
 * itself converts subtype 0 → subtype 4 as part of the data copy.
 * <p>
 * <b>Document read mapping</b>: Mongo's {@code Document.get(name)} returns
 * {@link Binary} (not {@link BsonBinary}) for binary fields; {@link #toUuid}
 * handles both plus raw {@code byte[]}.
 */
@ApiStatus.Internal
public final class BsonBinaries {

    private BsonBinaries() {}

    /** Encode a UUID as a 16-byte big-endian binary tagged {@code UUID_STANDARD}. */
    public static @NotNull BsonBinary uuidBinary(@NotNull UUID uuid) {
        byte[] bytes = new byte[16];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return new BsonBinary(BsonBinarySubType.UUID_STANDARD, bytes);
    }

    /**
     * Decode a UUID from a {@code Document.get(...)} result. Accepts
     * {@link UUID} (Mongo's UUID-aware codec maps subtype 4 directly to
     * {@code UUID} in some configurations), {@link Binary} (default
     * {@code Document.get} return for binary fields), {@link BsonBinary},
     * and raw {@code byte[]}. Any 16-byte payload is decoded as
     * big-endian (most/least-significant halves).
     */
    public static @Nullable UUID toUuid(@Nullable Object value) {
        if (value == null) return null;
        if (value instanceof UUID u) return u;
        if (value instanceof Binary b) return decode(b.getData());
        if (value instanceof BsonBinary b) return decode(b.getData());
        if (value instanceof byte[] bytes) return decode(bytes);
        throw new IllegalArgumentException("not a UUID-shaped BSON value: " + value.getClass().getName());
    }

    private static @NotNull UUID decode(byte @NotNull [] bytes) {
        if (bytes.length != 16) {
            throw new IllegalArgumentException("expected 16-byte UUID binary, got " + bytes.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new UUID(buf.getLong(), buf.getLong());
    }

    /**
     * Write a float[] as a binary blob with subtype 0 — little-endian IEEE-754
     * floats packed contiguously. This is the cheapest portable encoding for
     * vector search payloads; the per-backend search adapter unpacks it.
     * Phase 6 may switch to a backend-native vector type where one exists.
     */
    public static void writeFloatArray(@NotNull String name, float @NotNull [] values, @NotNull BsonWriter w) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : values) buf.putFloat(v);
        w.writeBinaryData(name, new BsonBinary(BsonBinarySubType.BINARY, buf.array()));
    }
}
