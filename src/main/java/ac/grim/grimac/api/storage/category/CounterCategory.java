package ac.grim.grimac.api.storage.category;

import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.CounterEvent;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.CounterOps;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Map;

@ApiStatus.Experimental
public interface CounterCategory<K> extends Category<CounterEvent<K>> {

    @NotNull Counter<K> kind();

    default @NotNull Operation<Long> get(@NotNull K key) {
        return new CounterOps.GetOp<>(this, key);
    }

    default @NotNull Operation<Long> incrementBy(@NotNull K key, long delta) {
        return new CounterOps.IncrementByOp<>(this, key, delta);
    }

    default @NotNull Operation<Long> setIfHigher(@NotNull K key, long value) {
        return new CounterOps.SetIfHigherOp<>(this, key, value);
    }

    default @NotNull Operation<Map<K, Long>> getMany(@NotNull Collection<K> keys) {
        return new CounterOps.GetManyOp<>(this, keys);
    }
}
