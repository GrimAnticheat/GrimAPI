package ac.grim.grimac.internal.storage.codec.bson;

import ac.grim.grimac.internal.storage.codec.MethodHandleCodec;
import ac.grim.grimac.internal.storage.codec.TypeTag;
import org.bson.BsonBinary;
import org.bson.types.Binary;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

/**
 * Shared decoder logic for {@link BsonCodecImpl} and {@link BsonTsCodecImpl}.
 * Bridges Mongo {@code Document}-shaped raw values to the typed values the
 * canonical constructor expects.
 * <p>
 * Centralises the conversion logic so JDBC + Redis subpackages can mirror the
 * same shape without copy-paste drift.
 */
@ApiStatus.Internal
public final class BsonDecodeHelper {

    private BsonDecodeHelper() {}

    /**
     * Decode {@code raw} (what {@code Document.get(name)} returned) into the
     * canonical-constructor argument value for field {@code i}.
     * <p>
     * Null {@code raw} delegates to {@code requireFieldOrDefault}, which
     * returns null for nullable fields and throws for required ones.
     */
    public static @Nullable Object decodeField(@NotNull MethodHandleCodec<?> codec, int i, @Nullable Object raw) {
        TypeTag tag = codec.typeTagAt(i);
        if (raw == null) {
            return codec.requireFieldOrDefault(i, null);
        }
        return switch (tag) {
            case INT          -> toInt(raw);
            case LONG         -> raw instanceof Date d ? d.getTime() : toLong(raw);
            case DOUBLE       -> toDouble(raw);
            case FLOAT        -> (float) toDouble(raw);
            case BOOLEAN      -> raw;
            case STRING       -> raw.toString();
            case BYTES        -> bytesFrom(raw);
            case UUID         -> BsonBinaries.toUuid(raw);
            case ENUM         -> codec.enumFromOrdinal(i, toInt(raw));
            case FLOAT_ARRAY  -> floatArrayFrom(raw);
            case NESTED_SEALED -> raw;
        };
    }

    private static int toInt(@NotNull Object o) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof Boolean b) return b ? 1 : 0;
        throw new IllegalArgumentException("expected int-like BSON value, got " + o.getClass().getName());
    }

    private static long toLong(@NotNull Object o) {
        if (o instanceof Number n) return n.longValue();
        if (o instanceof Date d) return d.getTime();
        throw new IllegalArgumentException("expected long-like BSON value, got " + o.getClass().getName());
    }

    private static double toDouble(@NotNull Object o) {
        if (o instanceof Number n) return n.doubleValue();
        throw new IllegalArgumentException("expected double-like BSON value, got " + o.getClass().getName());
    }

    private static byte @NotNull [] bytesFrom(@NotNull Object o) {
        if (o instanceof byte[] b) return b;
        if (o instanceof Binary b) return b.getData();
        if (o instanceof BsonBinary b) return b.getData();
        throw new IllegalArgumentException("expected byte[]-like BSON value, got " + o.getClass().getName());
    }

    /**
     * Reverse the {@link BsonBinaries#writeFloatArray} encoding: little-endian
     * IEEE-754 floats packed in a binary blob. Accepts an already-decoded
     * {@code float[]} (test/bulk-import path), {@code Binary}, {@code BsonBinary},
     * or raw {@code byte[]}.
     *
     * @throws IllegalArgumentException if the byte length isn't a multiple of 4
     */
    private static float @NotNull [] floatArrayFrom(@NotNull Object o) {
        if (o instanceof float[] fa) return fa;
        byte[] bytes;
        if (o instanceof byte[] b)          bytes = b;
        else if (o instanceof Binary b)     bytes = b.getData();
        else if (o instanceof BsonBinary b) bytes = b.getData();
        else throw new IllegalArgumentException(
            "expected float[]-shaped BSON value, got " + o.getClass().getName());
        if ((bytes.length & 3) != 0) {
            throw new IllegalArgumentException(
                "float[] binary length must be a multiple of 4, got " + bytes.length);
        }
        float[] out = new float[bytes.length / 4];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < out.length; i++) out[i] = buf.getFloat();
        return out;
    }
}
