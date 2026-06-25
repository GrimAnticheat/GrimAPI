package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Diagnostic metadata stamped on the current ownership row. None of these
 * fields participate in ownership decisions.
 */
@ApiStatus.Experimental
public record ServerOwnershipMetadata(
        @Nullable String serverName,
        @Nullable String hostname,
        @Nullable String grimVersion,
        @Nullable String serverVersionString) {
}
