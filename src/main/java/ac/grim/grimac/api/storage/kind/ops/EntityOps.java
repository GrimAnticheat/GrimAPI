package ac.grim.grimac.api.storage.kind.ops;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@ApiStatus.Experimental
public final class EntityOps {

    private EntityOps() {}

    public sealed interface Op<R> extends Operation<R>
            permits UpsertOp, GetByIdOp, GetManyOp, FindByIndexOp, PrefixIndexOp, DeleteByIdOp, DeleteByIndexOp, CountByIndexOp,
            SetIfSentinelOp, ReassignSessionOp {
    }

    /**
     * Move an existing session to a recovered storage startup and clear an
     * inferred crash closure atomically. Session identity, start time and metadata
     * are retained. Only a session owned by one of {@code previousStartups} may
     * move; a row already owned by {@code startupId} is left untouched, including
     * its closure. Returns 1 if moved, 0 if missing, already moved or conflicting.
     * Callers must hold the new server lease and prove the connection is still
     * alive in memory. Ordinary heartbeats must never use this operation.
     */
    public record ReassignSessionOp(
            @NotNull Category<?> category,
            @NotNull UUID sessionId,
            @NotNull Set<UUID> previousStartups,
            @NotNull UUID startupId) implements Op<Long> {
        public ReassignSessionOp {
            previousStartups = Set.copyOf(previousStartups);
            if (previousStartups.isEmpty()) throw new IllegalArgumentException("previousStartups");
        }
    }

    public record UpsertOp<R>(
            @NotNull Category<?> category,
            @NotNull R record) implements Op<Void> {}

    public record GetByIdOp<ID, R>(
            @NotNull Category<?> category,
            @NotNull ID id) implements Op<Optional<R>> {}

    public record GetManyOp<ID, R>(
            @NotNull Category<?> category,
            @NotNull Collection<ID> ids) implements Op<List<R>> {}

    public record FindByIndexOp<R>(
            @NotNull Category<?> category,
            @NotNull String indexName,
            @NotNull Object key,
            @Nullable Cursor cursor,
            int pageSize) implements Op<Page<R>> {}

    public record PrefixIndexOp<R>(
            @NotNull Category<?> category,
            @NotNull String indexName,
            @NotNull String prefix,
            @Nullable Cursor cursor,
            int pageSize) implements Op<Page<R>> {}

    public record DeleteByIdOp<ID>(
            @NotNull Category<?> category,
            @NotNull ID id) implements Op<Void> {}

    /**
     * Delete every row whose leading index column equals {@code key}.
     * Targets the index's first declared column — multi-column indexes
     * are matched on equality of the leading field only (mirrors
     * {@code FindByIndexOp}'s leading-column equality contract).
     */
    public record DeleteByIndexOp(
            @NotNull Category<?> category,
            @NotNull String indexName,
            @NotNull Object key) implements Op<Void> {}

    public record CountByIndexOp(
            @NotNull Category<?> category,
            @NotNull String indexName,
            @NotNull Object key) implements Op<Long> {}

    /**
     * Set {@code field} on every selected row whose {@code field} still equals
     * {@code sentinel}. Rows are selected by id when {@code indexName} is null,
     * otherwise by equality on the index's leading column. {@code value} wins;
     * when it is null the row's own {@code fromField} is copied instead.
     * Returns the number of rows changed. A repeat call changes nothing, so
     * concurrent callers never conflict. Meant for sentinel columns such as
     * {@code closed_at}; neither {@code field} nor the index may be backed by a
     * case-insensitive companion, which this operation does not maintain.
     */
    public record SetIfSentinelOp(
            @NotNull Category<?> category,
            @Nullable String indexName,
            @NotNull Object key,
            @NotNull String field,
            @NotNull Object sentinel,
            @Nullable Object value,
            @Nullable String fromField) implements Op<Long> {
        public SetIfSentinelOp {
            if ((value == null) == (fromField == null)) throw new IllegalArgumentException("exactly one of value or fromField");
        }
    }
}
