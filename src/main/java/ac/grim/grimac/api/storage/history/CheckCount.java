package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Count of violations for a single check, with its stable key and resolved display
 * name already looked up so Layer 3 does not need to carry the check registry.
 */
@ApiStatus.Experimental
public record CheckCount(
        int checkId,
        @NotNull String stableKey,
        @NotNull String displayName,
        int count) {
}
