package ac.grim.grimac.api.storage.admin;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Normalized explain-plan tree across backends. The {@link #raw()} blob
 * preserves backend-specific detail (Mongo {@code explain('executionStats')},
 * Postgres {@code EXPLAIN (FORMAT JSON, ANALYZE)}, etc.) for power users.
 */
@ApiStatus.Experimental
public record ExplainPlan(
        @NotNull String stage,
        long rowsExamined,
        long rowsReturned,
        @NotNull Duration estimatedTime,
        @Nullable String indexUsed,
        @NotNull Map<String, Object> details,
        @NotNull List<ExplainPlan> children,
        @NotNull Map<String, Object> raw) {

    public ExplainPlan {
        details = Map.copyOf(details);
        children = List.copyOf(children);
        raw = Map.copyOf(raw);
    }
}
