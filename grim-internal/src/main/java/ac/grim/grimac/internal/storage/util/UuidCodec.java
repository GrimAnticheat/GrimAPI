package ac.grim.grimac.internal.storage.util;

import org.jetbrains.annotations.ApiStatus;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 16-byte big-endian UUID codec shared by every backend that stores UUIDs as
 * fixed-width binary (SQLite BLOB, MySQL BINARY(16), Postgres BYTEA, Mongo
 * BsonBinary, Redis byte keys). A single canonical encoding keeps cross-backend
 * copy trivial: the same 16 bytes mean the same UUID everywhere.
 */
@ApiStatus.Internal
public final class UuidCodec {

    private UuidCodec() {}

    public static byte[] toBytes(UUID uuid) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    public static UUID fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalArgumentException("uuid bytes must be 16 bytes, got "
                    + (bytes == null ? "null" : bytes.length));
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new UUID(buf.getLong(), buf.getLong());
    }
}
