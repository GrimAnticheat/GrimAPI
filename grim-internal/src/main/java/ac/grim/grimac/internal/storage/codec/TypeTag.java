package ac.grim.grimac.internal.storage.codec;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Compact integer tag identifying how a record component is encoded /
 * decoded. The {@link MethodHandleCodec}'s hot loop switches on this rather
 * than doing a per-call {@code instanceof} chain — JIT compiles the switch
 * to a tableswitch, dispatch is single-digit nanoseconds.
 */
@ApiStatus.Internal
public enum TypeTag {
    INT,
    LONG,
    DOUBLE,
    FLOAT,
    BOOLEAN,
    STRING,
    BYTES,
    UUID,
    ENUM,
    FLOAT_ARRAY,        // for @Searchable(VECTOR)
    NESTED_SEALED;      // sealed-interface payload with discriminator

    /** Map a record component's static Java type to a tag. */
    public static @NotNull TypeTag of(@NotNull Class<?> type) {
        if (type == int.class || type == Integer.class) return INT;
        if (type == long.class || type == Long.class) return LONG;
        if (type == double.class || type == Double.class) return DOUBLE;
        if (type == float.class || type == Float.class) return FLOAT;
        if (type == boolean.class || type == Boolean.class) return BOOLEAN;
        if (type == String.class) return STRING;
        if (type == byte[].class) return BYTES;
        if (type == UUID.class) return UUID;
        if (type == float[].class) return FLOAT_ARRAY;
        if (type.isEnum()) return ENUM;
        if (type.isSealed()) return NESTED_SEALED;
        throw new IllegalArgumentException(
            "unsupported persistent field type: " + type.getName()
                + " (supported: int/long/double/float/boolean/String/byte[]/UUID/enum/float[]/sealed)");
    }
}
