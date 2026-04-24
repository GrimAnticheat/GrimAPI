package ac.grim.grimac.internal.storage.backend.sqlite;

import org.jetbrains.annotations.ApiStatus;

import java.nio.ByteBuffer;
import java.util.UUID;

@ApiStatus.Internal
final class UuidCodec {

    private UuidCodec() {}

    static byte[] toBytes(UUID uuid) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    static UUID fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new IllegalArgumentException("uuid bytes must be 16 bytes, got "
                    + (bytes == null ? "null" : bytes.length));
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new UUID(buf.getLong(), buf.getLong());
    }
}
