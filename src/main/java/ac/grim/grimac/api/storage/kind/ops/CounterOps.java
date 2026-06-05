package ac.grim.grimac.api.storage.kind.ops;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

@ApiStatus.Experimental
public final class CounterOps {

    private CounterOps() {}

    public sealed interface Op<R> extends Operation<R>
            permits GetOp, IncrementByOp, SetIfHigherOp, GetManyOp {
    }

    public record GetOp<K>(
            @NotNull Category<?> category,
            @NotNull K key) implements Op<Long> {}

    public record IncrementByOp<K>(
            @NotNull Category<?> category,
            @NotNull K key,
            long delta) implements Op<Long> {}

    public record SetIfHigherOp<K>(
            @NotNull Category<?> category,
            @NotNull K key,
            long value) implements Op<Long> {}

    public record GetManyOp<K>(
            @NotNull Category<?> category,
            @NotNull Collection<K> keys) implements Op<Map<K, Long>> {}
}
