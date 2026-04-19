package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * One row of a paged session listing. Pure data — every platform's command layer
 * converts this to its own chat format.
 * <p>
 * {@code pageOrdinal} is a 1-based label within the current page (newest-on-page =
 * {@code pageSize}, oldest-on-page = 1). Global chronological ordinals require an
 * extra count query and are deferred; see DESIGN_NOTES.md.
 */
@ApiStatus.Experimental
public record SessionSummary(
        @NotNull UUID sessionId,
        @NotNull UUID playerUuid,
        int pageOrdinal,
        long startedEpochMs,
        long lastActivityEpochMs,
        @Nullable String grimVersion,
        @Nullable String serverName,
        @Nullable String clientVersionString,
        @Nullable String clientBrand,
        long violationCount) {

    public long durationMs() {
        return Math.max(0, lastActivityEpochMs - startedEpochMs);
    }
}
