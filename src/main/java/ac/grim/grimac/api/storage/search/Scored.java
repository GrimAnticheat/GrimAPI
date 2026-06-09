package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * A search hit with normalized + raw score. Normalized score is 0..1
 * (1 = best in this result set); raw score is the backend's native
 * value (BM25, ts_rank, textScore).
 */
@ApiStatus.Experimental
public record Scored<R>(@NotNull R value, double normalizedScore, double rawScore) {
}
