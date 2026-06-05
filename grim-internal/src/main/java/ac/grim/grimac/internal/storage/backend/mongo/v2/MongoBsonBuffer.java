package ac.grim.grimac.internal.storage.backend.mongo.v2;

import org.bson.BsonBinaryWriter;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Per-handler-thread BSON encoding buffer. Each Disruptor handler thread
 * has exactly one instance via {@link ThreadLocal#withInitial}; the buffer
 * + writer pair is reused across writes, so the only per-write allocation
 * is the returned {@link RawBsonDocument} (which the driver promptly hands
 * to the network and discards).
 * <p>
 * Not thread-safe — that's intentional. The Disruptor handler thread model
 * guarantees a single writer per ring.
 */
@ApiStatus.Internal
final class MongoBsonBuffer {

    /**
     * Initial buffer size. ViolationRecord is ~140 bytes encoded; this
     * sizes for that plus a comfortable margin so 99% of writes don't
     * trigger {@code BasicOutputBuffer} growth. Buffer doubles when full.
     */
    private static final int INITIAL_CAPACITY = 256;

    private final @NotNull BasicOutputBuffer buf = new BasicOutputBuffer(INITIAL_CAPACITY);

    /**
     * Build a fresh writer over the reset buffer. The writer holds state
     * (current document depth, last-written field name) so a new one per
     * encode is mandatory; the underlying buffer is reused.
     */
    @NotNull BsonBinaryWriter writer() {
        buf.truncateToPosition(0);
        return new BsonBinaryWriter(buf);
    }

    /**
     * Snapshot the buffer's written bytes as a {@code RawBsonDocument}.
     * The driver inserts this directly — no Document intermediate, no
     * field-by-field rewrite.
     */
    @NotNull RawBsonDocument snapshot() {
        // BasicOutputBuffer.toByteArray() copies the live buffer once. The
        // resulting RawBsonDocument owns the copy; we're free to overwrite
        // the buffer on the next encode.
        return new RawBsonDocument(buf.toByteArray());
    }
}
