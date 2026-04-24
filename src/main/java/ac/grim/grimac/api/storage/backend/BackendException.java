package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public class BackendException extends Exception {

    public BackendException(String message) {
        super(message);
    }

    public BackendException(String message, Throwable cause) {
        super(message, cause);
    }

    public BackendException(Throwable cause) {
        super(cause);
    }
}
