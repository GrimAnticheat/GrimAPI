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
 * <p>
 * {@code clientVersion} is a PacketEvents protocol-version number (PVN) or
 * {@code -1} when unknown. Renderers resolve to a display string via
 * {@code ClientVersion.getById(pvn)} at the Layer-3 command-glue layer.
 * <p>
 * {@code uniqueCheckCount} is the number of distinct checks that flagged in this
 * session (shown as {@code [N]} in the design). {@code 0} when no violations.
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
        int clientVersion,
        @Nullable String clientBrand,
        long violationCount,
        int uniqueCheckCount) {

    public long durationMs() {
        return Math.max(0, lastActivityEpochMs - startedEpochMs);
    }
}
