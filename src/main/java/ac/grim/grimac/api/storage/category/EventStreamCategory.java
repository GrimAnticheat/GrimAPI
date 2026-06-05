package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * {@link Category} variant returned by registering an {@link EventStream}
 * Kind. Exposes the EventStream operation menu as default methods that
 * construct {@link Operation} records bound to this category.
 */
@ApiStatus.Experimental
public interface EventStreamCategory<E, R> extends Category<E> {

    @NotNull EventStream<E, R> kind();

    default @NotNull Operation<Page<R>> page(@NotNull String partition, @NotNull Object key,
                                             @Nullable Cursor cursor, int pageSize) {
        return new EventStreamOps.PageOp<>(this, partition, key, cursor, pageSize);
    }

    default @NotNull Operation<Page<R>> rangeByTime(long fromEpochMs, long toEpochMs,
                                                    @Nullable Cursor cursor, int pageSize) {
        return new EventStreamOps.RangeByTimeOp<>(this, fromEpochMs, toEpochMs, cursor, pageSize);
    }

    default @NotNull Operation<Long> count(@NotNull String partition, @NotNull Object key) {
        return new EventStreamOps.CountOp(this, partition, key);
    }

    default <K> @NotNull Operation<Map<K, Long>> countMany(@NotNull String partition,
                                                           @NotNull Collection<K> keys) {
        return new EventStreamOps.CountManyOp<>(this, partition, keys);
    }

    default @NotNull Operation<Long> countDistinct(@NotNull String partition, @NotNull Object key,
                                                   @NotNull String field) {
        return new EventStreamOps.CountDistinctOp(this, partition, key, field);
    }

    default @NotNull Operation<Void> deleteByPartition(@NotNull String partition, @NotNull Object key) {
        return new EventStreamOps.DeleteByPartitionOp(this, partition, key);
    }

    default @NotNull Operation<Void> deleteOlderThan(long cutoffEpochMs) {
        return new EventStreamOps.DeleteOlderThanOp(this, cutoffEpochMs);
    }
}
