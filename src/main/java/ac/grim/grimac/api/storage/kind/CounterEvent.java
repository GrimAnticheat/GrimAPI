package ac.grim.grimac.api.storage.kind;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable ring-buffer event slot for {@link Counter} writes. {@code delta} is
 * added to the current value; for absolute sets use {@code setIfHigher} via
 * the operation menu (not the ring path).
 */
@ApiStatus.Experimental
public final class CounterEvent<K> {

    public @Nullable K key;
    public long delta;

    public void clear() {
        key = null;
        delta = 0L;
    }
}
