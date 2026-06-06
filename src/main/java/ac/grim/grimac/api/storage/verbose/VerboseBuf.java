package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Primitive cursor facade for binary verbose payloads.
 *
 * <p>The buffer is backed by a reusable on-heap {@code byte[]} and deliberately
 * avoids Netty/NIO types. Writers append to the writer cursor and return this
 * for chaining; readers advance the reader cursor over the bytes already
 * written or wrapped.
 */
@ApiStatus.Experimental
public final class VerboseBuf {

    public static final int DEFAULT_CAPACITY = 64;
    public static final int MAX_STRING_BYTES = 65_535;

    private static final String UTF_8 = "UTF-8";

    private byte @NotNull [] data;
    private int readerIndex;
    private int writerIndex;

    public VerboseBuf() {
        this(DEFAULT_CAPACITY);
    }

    public VerboseBuf(int capacity) {
        if (capacity < 0) throw new IllegalArgumentException("capacity");
        this.data = new byte[capacity];
    }

    /**
     * Use {@code buffer} as writable scratch. The writer and reader cursors
     * start at zero.
     */
    public VerboseBuf(byte @NotNull [] buffer) {
        this.data = require(buffer, "buffer");
    }

    private VerboseBuf(byte @NotNull [] buffer, int readableLength) {
        this.data = require(buffer, "buffer");
        if (readableLength < 0 || readableLength > buffer.length) {
            throw new IllegalArgumentException("readableLength");
        }
        this.writerIndex = readableLength;
    }

    /** Wrap an existing payload for reading without copying. */
    public static @NotNull VerboseBuf wrap(byte @NotNull [] payload) {
        return new VerboseBuf(payload, payload.length);
    }

    /** Use an existing array as writable scratch without copying. */
    public static @NotNull VerboseBuf reuse(byte @NotNull [] buffer) {
        return new VerboseBuf(buffer);
    }

    public @NotNull VerboseBuf clear() {
        readerIndex = 0;
        writerIndex = 0;
        return this;
    }

    public @NotNull VerboseBuf rewind() {
        readerIndex = 0;
        return this;
    }

    public int remaining() {
        return writerIndex - readerIndex;
    }

    public int length() {
        return writerIndex;
    }

    public int capacity() {
        return data.length;
    }

    public int readerIndex() {
        return readerIndex;
    }

    public int writerIndex() {
        return writerIndex;
    }

    /**
     * Return the backing array. Only bytes {@code [0, length())} are part of
     * the current payload.
     */
    public byte @NotNull [] array() {
        return data;
    }

    public byte @NotNull [] toByteArray() {
        return Arrays.copyOf(data, writerIndex);
    }

    public @NotNull VerboseBuf f64(double v) {
        long bits = Double.doubleToLongBits(v);
        ensureWritable(8);
        for (int i = 0; i < 8; i++) {
            data[writerIndex++] = (byte) (bits >>> (i * 8));
        }
        return this;
    }

    public @NotNull VerboseBuf f32(float v) {
        int bits = Float.floatToIntBits(v);
        ensureWritable(4);
        for (int i = 0; i < 4; i++) {
            data[writerIndex++] = (byte) (bits >>> (i * 8));
        }
        return this;
    }

    /** Write an unsigned varint. Negative values belong on {@link #zz(int)}. */
    public @NotNull VerboseBuf vi(int v) {
        if (v < 0) throw new IllegalArgumentException("vi cannot encode negative value " + v);
        writeRawVarInt(v);
        return this;
    }

    public @NotNull VerboseBuf zz(int v) {
        writeRawVarInt((v << 1) ^ (v >> 31));
        return this;
    }

    /** Write a non-negative varlong. */
    public @NotNull VerboseBuf vl(long v) {
        if (v < 0L) throw new IllegalArgumentException("vl cannot encode negative value " + v);
        writeRawVarLong(v);
        return this;
    }

    public @NotNull VerboseBuf bool(boolean v) {
        writeByte(v ? 1 : 0);
        return this;
    }

    public @NotNull VerboseBuf str(@NotNull CharSequence v) {
        byte[] bytes = utf8(v.toString());
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string payload exceeds " + MAX_STRING_BYTES + " bytes");
        }
        vi(bytes.length);
        writeBytes(bytes);
        return this;
    }

    public double rf64() {
        requireReadable(8);
        long bits = 0L;
        for (int i = 0; i < 8; i++) {
            bits |= (long) (data[readerIndex++] & 0xFF) << (i * 8);
        }
        return Double.longBitsToDouble(bits);
    }

    public float rf32() {
        requireReadable(4);
        int bits = 0;
        for (int i = 0; i < 4; i++) {
            bits |= (data[readerIndex++] & 0xFF) << (i * 8);
        }
        return Float.intBitsToFloat(bits);
    }

    public int rvi() {
        int raw = readRawVarInt();
        if (raw < 0) throw new IllegalArgumentException("uvarint exceeds signed int range");
        return raw;
    }

    public int rzz() {
        int raw = readRawVarInt();
        return (raw >>> 1) ^ -(raw & 1);
    }

    public long rvl() {
        long raw = readRawVarLong();
        if (raw < 0L) throw new IllegalArgumentException("varlong exceeds signed long range");
        return raw;
    }

    public boolean rbool() {
        requireReadable(1);
        return data[readerIndex++] != 0;
    }

    public @NotNull String rstr() {
        int len = rvi();
        if (len > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string payload exceeds " + MAX_STRING_BYTES + " bytes");
        }
        requireReadable(len);
        String out = utf8(data, readerIndex, len);
        readerIndex += len;
        return out;
    }

    public void skip(int typeTag) {
        switch (typeTag) {
            case 0x01 -> skipFixed(8);
            case 0x02 -> skipFixed(4);
            case 0x03, 0x08 -> readRawVarInt();
            case 0x04 -> readRawVarInt();
            case 0x05 -> readRawVarLong();
            case 0x06 -> skipFixed(1);
            case 0x07 -> {
                int len = rvi();
                if (len > MAX_STRING_BYTES) {
                    throw new IllegalArgumentException("string payload exceeds " + MAX_STRING_BYTES + " bytes");
                }
                skipFixed(len);
            }
            default -> throw new IllegalArgumentException("unknown verbose type tag " + typeTag);
        }
    }

    void writeByte(int value) {
        ensureWritable(1);
        data[writerIndex++] = (byte) value;
    }

    void writeBytes(byte @NotNull [] bytes) {
        ensureWritable(bytes.length);
        System.arraycopy(bytes, 0, data, writerIndex, bytes.length);
        writerIndex += bytes.length;
    }

    int readUnsignedByte() {
        requireReadable(1);
        return data[readerIndex++] & 0xFF;
    }

    private void skipFixed(int bytes) {
        requireReadable(bytes);
        readerIndex += bytes;
    }

    private void writeRawVarInt(int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            writeByte((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        writeByte(v);
    }

    private int readRawVarInt() {
        int value = 0;
        int shift = 0;
        while (shift < 35) {
            int b = readUnsignedByte();
            if (shift == 28 && (b & 0xF0) != 0) {
                throw new IllegalArgumentException("varint exceeds 32 bits");
            }
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
        }
        throw new IllegalArgumentException("varint is too long");
    }

    private void writeRawVarLong(long value) {
        long v = value;
        while ((v & ~0x7FL) != 0L) {
            writeByte((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        writeByte((int) v);
    }

    private long readRawVarLong() {
        long value = 0L;
        int shift = 0;
        while (shift < 70) {
            int b = readUnsignedByte();
            if (shift == 63 && (b & 0xFE) != 0) {
                throw new IllegalArgumentException("varlong exceeds 64 bits");
            }
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return value;
            shift += 7;
        }
        throw new IllegalArgumentException("varlong is too long");
    }

    private void ensureWritable(int bytes) {
        int needed = writerIndex + bytes;
        if (needed <= data.length) return;
        int next = Math.max(DEFAULT_CAPACITY, data.length);
        while (next < needed) next = next + (next >>> 1) + 1;
        data = Arrays.copyOf(data, next);
    }

    private void requireReadable(int bytes) {
        if (bytes < 0 || remaining() < bytes) {
            throw new UnderflowException("verbose payload truncated");
        }
    }

    private static byte @NotNull [] utf8(@NotNull String value) {
        try {
            return value.getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 unavailable", e);
        }
    }

    private static @NotNull String utf8(byte @NotNull [] bytes, int offset, int length) {
        try {
            return new String(bytes, offset, length, UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError("UTF-8 unavailable", e);
        }
    }

    private static byte @NotNull [] require(byte @NotNull [] value, @NotNull String name) {
        if (value == null) throw new IllegalArgumentException(name);
        return value;
    }

    @ApiStatus.Experimental
    public static final class UnderflowException extends RuntimeException {
        public UnderflowException(@NotNull String message) {
            super(message);
        }
    }
}
