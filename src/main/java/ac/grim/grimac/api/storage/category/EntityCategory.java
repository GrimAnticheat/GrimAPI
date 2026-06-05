package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Page;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@ApiStatus.Experimental
public interface EntityCategory<ID, E, R> extends Category<E> {

    @NotNull Entity<ID, E, R> kind();

    default @NotNull Operation<Optional<R>> getById(@NotNull ID id) {
        return new EntityOps.GetByIdOp<>(this, id);
    }

    default @NotNull Operation<List<R>> getMany(@NotNull Collection<ID> ids) {
        return new EntityOps.GetManyOp<>(this, ids);
    }

    default @NotNull Operation<Page<R>> findByIndex(@NotNull String indexName, @NotNull Object key,
                                                    @Nullable Cursor cursor, int pageSize) {
        return new EntityOps.FindByIndexOp<>(this, indexName, key, cursor, pageSize);
    }

    default @NotNull Operation<Page<R>> prefixIndex(@NotNull String indexName, @NotNull String prefix,
                                                    @Nullable Cursor cursor, int pageSize) {
        return new EntityOps.PrefixIndexOp<>(this, indexName, prefix, cursor, pageSize);
    }

    default @NotNull Operation<Void> deleteById(@NotNull ID id) {
        return new EntityOps.DeleteByIdOp<>(this, id);
    }

    default @NotNull Operation<Long> countByIndex(@NotNull String indexName, @NotNull Object key) {
        return new EntityOps.CountByIndexOp(this, indexName, key);
    }
}
