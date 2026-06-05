package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.kind.KeyValueEvent;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

@ApiStatus.Experimental
public interface KeyValueScopedCategory<S, V> extends Category<KeyValueEvent<S, V>> {

    @NotNull KeyValueScoped<S, V> kind();

    default @NotNull Operation<Optional<V>> get(@NotNull S scope, @NotNull String scopeKey,
                                                @NotNull String key) {
        return new KeyValueScopedOps.GetOp<>(this, scope, scopeKey, key);
    }

    default @NotNull Operation<Map<String, V>> getAll(@NotNull S scope, @NotNull String scopeKey) {
        return new KeyValueScopedOps.GetAllOp<>(this, scope, scopeKey);
    }

    default @NotNull Operation<Void> put(@NotNull S scope, @NotNull String scopeKey,
                                         @NotNull String key, @NotNull V value) {
        return new KeyValueScopedOps.PutOp<>(this, scope, scopeKey, key, value);
    }

    default @NotNull Operation<Void> putAll(@NotNull S scope, @NotNull String scopeKey,
                                            @NotNull Map<String, V> values) {
        return new KeyValueScopedOps.PutAllOp<>(this, scope, scopeKey, values);
    }

    default @NotNull Operation<Void> remove(@NotNull S scope, @NotNull String scopeKey,
                                            @NotNull String key) {
        return new KeyValueScopedOps.RemoveOp<>(this, scope, scopeKey, key);
    }

    default @NotNull Operation<Void> removeAll(@NotNull S scope, @NotNull String scopeKey) {
        return new KeyValueScopedOps.RemoveAllOp<>(this, scope, scopeKey);
    }

    default @NotNull Operation<Long> count(@NotNull S scope, @NotNull String scopeKey) {
        return new KeyValueScopedOps.CountOp<>(this, scope, scopeKey);
    }
}
