package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.model.BlobRef;
import org.jetbrains.annotations.ApiStatus;

import java.io.IOException;

@ApiStatus.Experimental
public class NoSuchBlobException extends IOException {

    public NoSuchBlobException(BlobRef ref) {
        super("blob not found: " + ref);
    }
}
