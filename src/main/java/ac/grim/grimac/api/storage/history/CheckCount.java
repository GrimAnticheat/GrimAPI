package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Count of violations for a single check. The stable key and resolved display
 * name are pre-filled, so callers don't need their own copy of the check
 * registry just to render a history row.
 */
@ApiStatus.Experimental
public record CheckCount(
        int checkId,
        @NotNull String stableKey,
        @NotNull String displayName,
        int count) {
}
