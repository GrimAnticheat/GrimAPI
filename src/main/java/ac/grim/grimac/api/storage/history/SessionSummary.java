package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * One row of a paged session listing. Pure data — every platform's command layer
 * converts this to its own chat format.
 * <p>
 * {@code sessionOrdinal} is the <strong>global chronological</strong> ordinal —
 * Session 1 is the player's very first session ever, Session K is their most
 * recent. This differs from a page-local label: two sessions on different pages
 * never share an ordinal. Computing it costs one {@code countSessions} query;
 * the service handles that bookkeeping.
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
        int sessionOrdinal,
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
