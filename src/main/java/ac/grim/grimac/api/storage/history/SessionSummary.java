package ac.grim.grimac.api.storage.history;

import ac.grim.grimac.api.storage.model.SessionRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * One row of a paged session listing. Pure data — callers convert to whatever
 * chat / UI format they use.
 * <p>
 * {@code sessionOrdinal} is the <strong>global chronological</strong> ordinal:
 * Session 1 is the player's very first session ever, Session K is their most
 * recent. Two sessions on different pages never share an ordinal. Computing
 * it costs one {@code countSessions} query — the service handles that
 * bookkeeping so callers don't have to.
 * <p>
 * {@code clientVersion} is a PacketEvents protocol-version number (PVN) or
 * {@code -1} when unknown. Renderers resolve to a display string via
 * {@code ClientVersion.getById(pvn)} at the point where chat components
 * are built — the storage API is PacketEvents-free on purpose.
 * <p>
 * {@code uniqueCheckCount} is the number of distinct checks that flagged in
 * this session (shown as {@code [N]} in the session-list UI). {@code 0} when
 * there were no violations.
 * <p>
 * {@code closedAtEpochMs} uses {@code 0L} as the "still open" sentinel —
 * mirrors {@link SessionRecord#OPEN}. Set to a real timestamp when the
 * session ends (graceful disconnect path, or the next startup's crash
 * sweep). Use {@link #isClosed} rather than comparing to 0 directly.
 * Renderers compare {@code closedAtEpochMs} to {@code lastActivityEpochMs}
 * to distinguish graceful (strictly greater) from crashed (equal).
 */
@ApiStatus.Experimental
public record SessionSummary(
        @NotNull UUID sessionId,
        @NotNull UUID playerUuid,
        int sessionOrdinal,
        long startedEpochMs,
        long lastActivityEpochMs,
        long closedAtEpochMs,
        @Nullable String grimVersion,
        @Nullable String serverName,
        int clientVersion,
        @Nullable String clientBrand,
        long violationCount,
        int uniqueCheckCount) {

    public long durationMs() {
        return Math.max(0, lastActivityEpochMs - startedEpochMs);
    }

    /** {@code true} when {@code closedAtEpochMs} carries a real close timestamp ({@code != 0L}). */
    public boolean isClosed() {
        return closedAtEpochMs != SessionRecord.OPEN;
    }

    /**
     * True when the session ended without a graceful disconnect — the
     * crash-sweep stamped {@code closed_at} from {@code last_activity},
     * so they're equal. Renderers use this to mark the session as crashed.
     * False while ongoing ({@code isClosed() == false}) or gracefully closed
     * (closedAt > lastActivity by however long the close-path's "now" was
     * after the last heartbeat).
     */
    public boolean endedUnexpectedly() {
        return isClosed() && closedAtEpochMs == lastActivityEpochMs;
    }
}
