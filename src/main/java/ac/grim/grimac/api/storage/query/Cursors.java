package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Canonical encoding for {@link Cursor} tokens. Same shape across every
 * backend: a small header + an ordered key + a length-prefixed id byte
 * array, base64-URL encoded. Adapters decode to their native paging
 * primitives.
 * <p>
 * Two wire-format variants share the same overall envelope; the leading
 * schema byte selects the decoder.
 * <p>
 * Schema {@code 0x01} ({@link #SCHEMA_ORDERED_PAIR}) — long ordered key:
 * <pre>
 *   [ 1 byte  : schema tag = 0x01 ]
 *   [ 8 bytes : ordered key, big-endian long (e.g. occurred_at epoch ms) ]
 *   [ 1 byte  : id byte length (0..255) ]
 *   [ N bytes : id payload ]
 * </pre>
 * Total: 10 + N bytes (~36 chars base64-URL for a 16-byte UUID id).
 * <p>
 * Schema {@code 0x02} ({@link #SCHEMA_TYPED_PAIR}) — typed ordered key:
 * <pre>
 *   [ 1 byte  : schema tag = 0x02 ]
 *   [ 1 byte  : type tag — STRING / BINARY / DOUBLE / DATE ]
 *   [ 2 bytes : ordered-key length, big-endian uint16 ]
 *   [ M bytes : ordered-key payload (see per-type encoding below) ]
 *   [ 1 byte  : id byte length (0..255) ]
 *   [ N bytes : id payload ]
 * </pre>
 * Per-type ordered-key payload encoding:
 * <ul>
 *   <li>{@link #TYPE_STRING} — UTF-8 bytes of the string</li>
 *   <li>{@link #TYPE_BINARY} — 1 byte BSON binary subtype + N raw bytes
 *       (subtype preserved so UUID_STANDARD (0x04) cursors compare
 *       identically to the stored {@code BsonBinary})</li>
 *   <li>{@link #TYPE_DOUBLE} — IEEE-754 big-endian, exactly 8 bytes</li>
 *   <li>{@link #TYPE_DATE} — epoch milliseconds big-endian, exactly 8
 *       bytes (consumer reconstructs {@code java.util.Date} so the filter
 *       compares Date-to-Date rather than long-to-Date)</li>
 * </ul>
 * Used by Entity adapters when an index's ordered column isn't a plain
 * Java {@code long}. Carries the raw key bytes (plus subtype for binary,
 * plus a type tag for everything else) so the consuming adapter can
 * issue a typed range filter rather than reducing the value through a
 * lossy hash.
 * <p>
 * Cursors remain opaque to consumers; round-trippable across
 * mongo / postgres / mysql / sqlite / redis without re-encoding.
 */
@ApiStatus.Experimental
public final class Cursors {

    /** Schema marker for {@link #encode(long, byte[])} (long ordered key). */
    public static final byte SCHEMA_ORDERED_PAIR = 0x01;
    /** Schema marker for {@link #encodeTyped(byte, byte[], byte[])} (typed ordered key). */
    public static final byte SCHEMA_TYPED_PAIR   = 0x02;

    /** Typed-pair type tag: STRING ordered key (UTF-8 bytes). */
    public static final byte TYPE_STRING = 0x01;
    /** Typed-pair type tag: BINARY ordered key (1 byte subtype + raw bytes). */
    public static final byte TYPE_BINARY = 0x02;
    /** Typed-pair type tag: DOUBLE ordered key (IEEE-754 big-endian, 8 bytes). */
    public static final byte TYPE_DOUBLE = 0x03;
    /** Typed-pair type tag: DATE ordered key (epoch ms big-endian, 8 bytes). */
    public static final byte TYPE_DATE   = 0x04;

    /** Max ordered-key payload for schema 0x02 (header field is a uint16). */
    public static final int MAX_TYPED_ORDERED_LEN = 0xFFFF;
    /**
     * Defensive upper bound on a base64-decoded cursor payload. 4 KiB is
     * far above any legitimate cursor (uint16 ordered + 255 id ~= 64 KiB
     * worst case; real cursors are tens of bytes), but keeps a malicious
     * caller from forcing oversized allocations during {@link #base64}.
     */
    public static final int MAX_PAYLOAD_BYTES = 4 * 1024;

    private Cursors() {}

    /**
     * Encode a {@code (orderedKey, idBytes)} pair with a long ordered key.
     * The ordered key is typically a timestamp in epoch milliseconds for
     * {@code EventStream}, or a secondary-index long-valued column for
     * {@code Entity}; the id bytes are the per-row primary key (e.g. a
     * UUID's 16-byte big-endian form).
     */
    public static @NotNull Cursor encode(long orderedKey, byte @NotNull [] idBytes) {
        if (idBytes.length > 255) {
            throw new IllegalArgumentException("id payload > 255 bytes: " + idBytes.length);
        }
        ByteBuffer buf = ByteBuffer.allocate(10 + idBytes.length);
        buf.put(SCHEMA_ORDERED_PAIR);
        buf.putLong(orderedKey);
        buf.put((byte) idBytes.length);
        buf.put(idBytes);
        return new Cursor(Base64.getUrlEncoder().withoutPadding().encodeToString(buf.array()));
    }

    /**
     * Encode a {@code (typeTag, orderedBytes, idBytes)} triple for
     * non-long ordered keys (strings, binary, doubles, dates). The
     * {@code typeTag} MUST be one of {@link #TYPE_STRING},
     * {@link #TYPE_BINARY}, {@link #TYPE_DOUBLE}, or {@link #TYPE_DATE};
     * unknown type tags are rejected at encode time to prevent producing
     * cursors no decoder can interpret.
     * <p>
     * The {@code orderedBytes} format is per-type — see the class-level
     * javadoc.
     */
    public static @NotNull Cursor encodeTyped(byte typeTag, byte @NotNull [] orderedBytes, byte @NotNull [] idBytes) {
        if (!isKnownTypeTag(typeTag)) {
            throw new IllegalArgumentException(
                "unknown typed-cursor type tag 0x" + Integer.toHexString(typeTag & 0xff));
        }
        if (orderedBytes.length > MAX_TYPED_ORDERED_LEN) {
            throw new IllegalArgumentException(
                "typed ordered payload > " + MAX_TYPED_ORDERED_LEN + " bytes: " + orderedBytes.length);
        }
        if (idBytes.length > 255) {
            throw new IllegalArgumentException("id payload > 255 bytes: " + idBytes.length);
        }
        // Reject any encoded payload that would later be rejected by the
        // decoder. Without this, encode produces cursors that fail
        // immediately on first decode — caller never sees the size limit
        // and writes bad cursor tokens to logs / clients.
        int total = 5 + orderedBytes.length + idBytes.length;
        if (total > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                "encoded typed cursor exceeds MAX_PAYLOAD_BYTES (" + MAX_PAYLOAD_BYTES + "): " + total);
        }
        ByteBuffer buf = ByteBuffer.allocate(5 + orderedBytes.length + idBytes.length);
        buf.put(SCHEMA_TYPED_PAIR);
        buf.put(typeTag);
        buf.putShort((short) orderedBytes.length);
        buf.put(orderedBytes);
        buf.put((byte) idBytes.length);
        buf.put(idBytes);
        return new Cursor(Base64.getUrlEncoder().withoutPadding().encodeToString(buf.array()));
    }

    /**
     * Decode a long-pair cursor (schema {@link #SCHEMA_ORDERED_PAIR}).
     * Throws if the cursor uses a different schema — callers that may see
     * either variant should peek with {@link #peekSchema(Cursor)} first.
     */
    public static @NotNull Decoded decode(@NotNull Cursor cursor) {
        byte[] raw = base64(cursor);
        if (raw.length < 10) {
            throw new IllegalArgumentException("cursor payload too short (" + raw.length + " bytes)");
        }
        ByteBuffer buf = ByteBuffer.wrap(raw);
        byte tag = buf.get();
        if (tag != SCHEMA_ORDERED_PAIR) {
            throw new IllegalArgumentException("unknown cursor schema tag: " + tag);
        }
        long orderedKey = buf.getLong();
        int idLen = Byte.toUnsignedInt(buf.get());
        if (buf.remaining() != idLen) {
            throw new IllegalArgumentException(
                "cursor id length mismatch: header=" + idLen + " remaining=" + buf.remaining());
        }
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        return new Decoded(orderedKey, idBytes);
    }

    /**
     * Decode a typed-pair cursor (schema {@link #SCHEMA_TYPED_PAIR}).
     * Throws if the cursor uses a different schema or carries an
     * unknown type tag.
     */
    public static @NotNull DecodedTyped decodeTyped(@NotNull Cursor cursor) {
        byte[] raw = base64(cursor);
        if (raw.length < 5) {
            throw new IllegalArgumentException("typed cursor payload too short (" + raw.length + " bytes)");
        }
        ByteBuffer buf = ByteBuffer.wrap(raw);
        byte schema = buf.get();
        if (schema != SCHEMA_TYPED_PAIR) {
            throw new IllegalArgumentException("expected typed-pair schema (0x02), got 0x"
                + Integer.toHexString(schema & 0xff));
        }
        byte typeTag = buf.get();
        if (!isKnownTypeTag(typeTag)) {
            throw new IllegalArgumentException(
                "unknown typed-cursor type tag 0x" + Integer.toHexString(typeTag & 0xff));
        }
        int orderedLen = Short.toUnsignedInt(buf.getShort());
        if (buf.remaining() < orderedLen + 1) {
            throw new IllegalArgumentException("typed cursor truncated: ordered=" + orderedLen
                + " remaining=" + buf.remaining());
        }
        byte[] orderedBytes = new byte[orderedLen];
        buf.get(orderedBytes);
        int idLen = Byte.toUnsignedInt(buf.get());
        if (buf.remaining() != idLen) {
            throw new IllegalArgumentException("typed cursor id length mismatch: header=" + idLen
                + " remaining=" + buf.remaining());
        }
        byte[] idBytes = new byte[idLen];
        buf.get(idBytes);
        return new DecodedTyped(typeTag, orderedBytes, idBytes);
    }

    /**
     * Peek the schema byte without fully decoding the payload. Lets
     * adapters dispatch between {@link #decode(Cursor)} and
     * {@link #decodeTyped(Cursor)} when the cursor's variant isn't
     * known statically.
     */
    public static byte peekSchema(@NotNull Cursor cursor) {
        byte[] raw = base64(cursor);
        if (raw.length < 1) throw new IllegalArgumentException("empty cursor payload");
        return raw[0];
    }

    private static byte @NotNull [] base64(@NotNull Cursor cursor) {
        // Guard against oversized base64 inputs before allocating the
        // decoded buffer. base64 inflates 3 bytes → 4 chars, so the
        // decoded length is bounded by (token.length * 3) / 4.
        String token = cursor.token();
        if ((long) token.length() * 3 / 4 > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                "cursor payload exceeds max " + MAX_PAYLOAD_BYTES + " bytes (token length " + token.length() + ")");
        }
        try {
            byte[] raw = Base64.getUrlDecoder().decode(token);
            if (raw.length > MAX_PAYLOAD_BYTES) {
                throw new IllegalArgumentException(
                    "cursor payload exceeds max " + MAX_PAYLOAD_BYTES + " bytes: " + raw.length);
            }
            return raw;
        } catch (IllegalArgumentException e) {
            // Re-throw with the cursor's token suffix for debuggability,
            // but cap the token in the message so we don't echo a huge
            // attacker-controlled string into the logs.
            String safe = token.length() > 64 ? token.substring(0, 64) + "..." : token;
            throw new IllegalArgumentException("cursor is not valid base64-url: " + safe, e);
        }
    }

    private static boolean isKnownTypeTag(byte t) {
        return t == TYPE_STRING || t == TYPE_BINARY || t == TYPE_DOUBLE || t == TYPE_DATE;
    }

    /** Decoded long-pair cursor returned by {@link #decode(Cursor)}. */
    public record Decoded(long orderedKey, byte @NotNull [] idBytes) {}

    /** Decoded typed-pair cursor returned by {@link #decodeTyped(Cursor)}. */
    public record DecodedTyped(byte typeTag, byte @NotNull [] orderedBytes, byte @NotNull [] idBytes) {}
}
