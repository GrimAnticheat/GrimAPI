package ac.grim.grimac.internal.storage.retention;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Deletes rows whose {@code fields} are all older than {@code cutoffEpochMs}, at most
 * {@code limit} of them ({@code 0} = no limit), and returns how many it deleted. Handled
 * by the SQL and Mongo entity and event-stream adapters; retention uses it so a large
 * backlog is deleted in short batches that never hold a table, or a single-writer
 * backend's writer thread, for long.
 */
@ApiStatus.Internal
public record DeleteOlderThanOp(
        @NotNull Category<?> category,
        @NotNull List<String> fields,
        long cutoffEpochMs,
        int limit) implements Operation<Long> {

    public DeleteOlderThanOp {
        fields = List.copyOf(fields);
        if (fields.isEmpty()) throw new IllegalArgumentException("fields");
        if (limit < 0) throw new IllegalArgumentException("limit");
    }
}
