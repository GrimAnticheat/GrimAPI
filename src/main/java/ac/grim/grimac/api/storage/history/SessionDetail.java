package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;

/**
 * All data needed to render a single session's detail view. The full violation list
 * and the pre-aggregated {@code buckets} are both provided; renderers decide which
 * to show (e.g. summary vs. detailed mode).
 * <p>
 * {@code clientVersion} is a PacketEvents PVN or {@code -1} when unknown — see
 * {@link SessionSummary} for the rationale.
 */
@ApiStatus.Experimental
public record SessionDetail(
        @NotNull UUID sessionId,
        @NotNull UUID playerUuid,
        int sessionOrdinal,
        long startedEpochMs,
        long lastActivityEpochMs,
        @Nullable String grimVersion,
        @Nullable String serverName,
        int clientVersion,
        @Nullable String clientBrand,
        long bucketSizeMs,
        int uniqueCheckCount,
        @NotNull List<CheckBucket> buckets,
        @NotNull List<ViolationEntry> violations) {

    public SessionDetail {
        buckets = buckets == null ? List.of() : List.copyOf(buckets);
        violations = violations == null ? List.of() : List.copyOf(violations);
    }

    public long durationMs() {
        return Math.max(0, lastActivityEpochMs - startedEpochMs);
    }
}
