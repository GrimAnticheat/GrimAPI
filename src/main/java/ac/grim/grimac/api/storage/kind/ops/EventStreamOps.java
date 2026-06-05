package ac.grim.grimac.api.storage.kind.ops;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * Operation records for {@code EventStream} stores. The per-Kind dispatcher
 * pattern-matches on these inside the backend's adapter.
 */
@ApiStatus.Experimental
public final class EventStreamOps {

    private EventStreamOps() {}

    public sealed interface Op<R> extends Operation<R>
            permits PageOp, RangeByTimeOp, CountOp, CountManyOp, CountDistinctOp,
                    DeleteByPartitionOp, DeleteOlderThanOp {
    }

    public record PageOp<R>(
            @NotNull Category<?> category,
            @NotNull String partition,
            @NotNull Object key,
            @Nullable Cursor cursor,
            int pageSize) implements Op<Page<R>> {}

    public record RangeByTimeOp<R>(
            @NotNull Category<?> category,
            long fromEpochMs,
            long toEpochMs,
            @Nullable Cursor cursor,
            int pageSize) implements Op<Page<R>> {}

    public record CountOp(
            @NotNull Category<?> category,
            @NotNull String partition,
            @NotNull Object key) implements Op<Long> {}

    public record CountManyOp<K>(
            @NotNull Category<?> category,
            @NotNull String partition,
            @NotNull Collection<K> keys) implements Op<Map<K, Long>> {}

    public record CountDistinctOp(
            @NotNull Category<?> category,
            @NotNull String partition,
            @NotNull Object key,
            @NotNull String field) implements Op<Long> {}

    public record DeleteByPartitionOp(
            @NotNull Category<?> category,
            @NotNull String partition,
            @NotNull Object key) implements Op<Void> {}

    public record DeleteOlderThanOp(
            @NotNull Category<?> category,
            long cutoffEpochMs) implements Op<Void> {}
}
