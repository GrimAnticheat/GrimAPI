package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.BlobRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Blob storage SPI. Semantics: {@link #put} streams (no "load into memory first");
 * {@link #get} returns a stream the caller closes. Missing keys throw
 * {@link NoSuchBlobException} from get (checked) and return false from has.
 *
 * <p>Two producer modes:
 * <ul>
 *   <li>{@link #put(String, InputStream, BlobPutOptions)} - the caller has the
 *       full stream up front. Backend reads until EOF.</li>
 *   <li>{@link #put(String, OutputStreamConsumer, BlobPutOptions)} - streaming
 *       producer. Backend hands the producer a write-only stream; the producer
 *       writes incrementally and returns. Backend finalises the blob on
 *       producer return.</li>
 * </ul>
 */
@ApiStatus.Experimental
public interface BlobStore {

    /**
     * Stable backend id stamped into {@link BlobRef}s produced by this store.
     */
    default String backendId() {
        return getClass().getName();
    }

    BlobRef put(String namespace, InputStream in, BlobPutOptions opts) throws IOException;

    /**
     * Writes a blob at a deterministic key inside {@code namespace}. Backends
     * should publish either the complete blob or the pre-existing blob at that
     * key; partial writes must not become visible.
     */
    default BlobRef put(String namespace, String key, InputStream in, BlobPutOptions opts) throws IOException {
        throw new UnsupportedOperationException("deterministic blob keys are not supported by this backend");
    }

    /**
     * Streaming-producer overload. The {@link OutputStreamConsumer} writes
     * incrementally to the supplied {@link OutputStream}; the backend finalises
     * the blob on producer return (atomic rename for filesystem, multipart
     * complete for S3, etc.).
     *
     * <p>If the producer throws, the backend MUST NOT publish a partial blob -
     * the in-flight upload is aborted and the exception propagates.
     */
    BlobRef put(String namespace, OutputStreamConsumer producer, BlobPutOptions opts) throws IOException;

    default BlobRef put(String namespace, String key, OutputStreamConsumer producer, BlobPutOptions opts)
            throws IOException {
        throw new UnsupportedOperationException("deterministic blob keys are not supported by this backend");
    }

    InputStream get(BlobRef ref) throws IOException;

    default InputStream get(String namespace, String key) throws IOException {
        return get(new BlobRef(backendId(), namespace + "/" + key, -1L, null));
    }

    long size(BlobRef ref) throws IOException;

    default long size(String namespace, String key) throws IOException {
        return size(new BlobRef(backendId(), namespace + "/" + key, -1L, null));
    }

    boolean has(BlobRef ref) throws IOException;

    default boolean has(String namespace, String key) throws IOException {
        return has(new BlobRef(backendId(), namespace + "/" + key, -1L, null));
    }

    void delete(BlobRef ref) throws IOException;

    default void delete(String namespace, String key) throws IOException {
        delete(new BlobRef(backendId(), namespace + "/" + key, -1L, null));
    }

    Stream<BlobRef> list(String namespace, String prefix) throws IOException;

    @FunctionalInterface
    interface OutputStreamConsumer {
        /** Writes the blob's bytes to {@code out}. Caller does not close {@code out}; the backend handles finalisation. */
        void writeTo(OutputStream out) throws IOException;
    }

    record BlobPutOptions(
            @Nullable String contentType,
            long sizeHintBytes,
            @Nullable Map<String, String> metadata) {}
}
