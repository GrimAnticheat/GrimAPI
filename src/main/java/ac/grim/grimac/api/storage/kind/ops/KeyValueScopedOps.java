package ac.grim.grimac.api.storage.kind.ops;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

@ApiStatus.Experimental
public final class KeyValueScopedOps {

    private KeyValueScopedOps() {}

    public sealed interface Op<R> extends Operation<R>
            permits GetOp, GetAllOp, PutOp, PutAllOp, RemoveOp, RemoveAllOp, CountOp {
    }

    public record GetOp<S, V>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey,
            @NotNull String key) implements Op<Optional<V>> {}

    public record GetAllOp<S, V>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey) implements Op<Map<String, V>> {}

    public record PutOp<S, V>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey,
            @NotNull String key,
            @NotNull V value) implements Op<Void> {}

    public record PutAllOp<S, V>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey,
            @NotNull Map<String, V> values) implements Op<Void> {}

    public record RemoveOp<S>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey,
            @NotNull String key) implements Op<Void> {}

    public record RemoveAllOp<S>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey) implements Op<Void> {}

    public record CountOp<S>(
            @NotNull Category<?> category,
            @NotNull S scope,
            @NotNull String scopeKey) implements Op<Long> {}
}
