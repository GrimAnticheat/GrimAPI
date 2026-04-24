package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.BlobRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Blob storage SPI. Semantics: {@link #put} streams (no "load into memory first");
 * {@link #get} returns a stream the caller closes. Missing keys throw
 * {@link NoSuchBlobException} from get (checked) and return false from has.
 */
@ApiStatus.Experimental
public interface BlobStore {

    BlobRef put(String namespace, InputStream in, BlobPutOptions opts) throws IOException;

    InputStream get(BlobRef ref) throws IOException;

    long size(BlobRef ref) throws IOException;

    boolean has(BlobRef ref) throws IOException;

    void delete(BlobRef ref) throws IOException;

    Stream<BlobRef> list(String namespace, String prefix) throws IOException;

    record BlobPutOptions(
            @Nullable String contentType,
            long sizeHintBytes,
            @Nullable Map<String, String> metadata) {}
}
