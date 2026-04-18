package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.BlobRef;
import org.jetbrains.annotations.ApiStatus;

import java.io.IOException;
import java.io.InputStream;

/**
 * Scoped blob handle given to an extension. Resolves only under {@code ext/<extId>/}.
 * Runtime guard + static analyzer (future) enforce this.
 */
@ApiStatus.Experimental
public interface BlobStoreHandle {

    BlobRef put(String subKey, InputStream in, BlobStore.BlobPutOptions opts) throws IOException;

    InputStream get(BlobRef ref) throws IOException;

    void delete(BlobRef ref) throws IOException;
}
