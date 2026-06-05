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

@ApiStatus.Experimental
public final class EntityOps {

    private EntityOps() {}

    public sealed interface Op<R> extends Operation<R>
            permits GetByIdOp, GetManyOp, FindByIndexOp, PrefixIndexOp, DeleteByIdOp, DeleteByIndexOp, CountByIndexOp {
    }

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
}
