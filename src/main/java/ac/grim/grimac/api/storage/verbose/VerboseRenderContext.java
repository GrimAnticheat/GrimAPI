package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime context available when rendering stored verbose payloads.
 */
@ApiStatus.Experimental
public record VerboseRenderContext(int clientVersionPvn, @Nullable String serverVersion) {
}
