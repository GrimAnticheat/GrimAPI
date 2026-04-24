package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;

/**
 * Shape of the {@code verbose} payload attached to a violation. Persisted
 * as a tinyint code ({@link #code()}) on backends that support it, so that
 * new values can be added without re-stringifying every stored row.
 * <p>
 * Code {@code 2} is reserved for a future dedup-reference value (verbose
 * column becomes an id into a hash-interned side table). Readers must
 * tolerate unknown codes by defaulting to {@link #TEXT} and logging; the
 * schema contract is that codes are strictly forward-adding.
 */
@ApiStatus.Experimental
public enum VerboseFormat {
    TEXT((byte) 0),
    STRUCTURED_V1((byte) 1);

    private final byte code;

    VerboseFormat(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static VerboseFormat fromCode(int code) {
        for (VerboseFormat f : values()) {
            if (f.code == code) return f;
        }
        return TEXT;
    }
}
