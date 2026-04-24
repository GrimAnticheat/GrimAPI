package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;

import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Environment handed to a {@link Backend} on {@link Backend#init(BackendContext)}.
 * Provides resources the backend needs (config, logger, data directory) without
 * pulling in platform-specific dependencies.
 */
@ApiStatus.Experimental
public interface BackendContext {

    BackendConfig config();

    Logger logger();

    /**
     * Data directory root (relative path usable for file-based backends like SQLite).
     * File-based backends resolve their own files under this directory.
     */
    Path dataDirectory();
}
