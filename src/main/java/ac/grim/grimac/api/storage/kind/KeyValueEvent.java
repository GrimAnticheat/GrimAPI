package ac.grim.grimac.api.storage.kind;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Mutable ring-buffer event slot for {@link KeyValueScoped} writes. Slots are
 * pre-allocated and recycled — never retain a reference past the configurer's
 * return.
 * <p>
 * Not {@code final}: {@code SettingEvent} extends this so the v2 KV adapters
 * can accept SETTING category slots through the standard
 * {@code StorageEventHandler<KeyValueEvent<?, ?>>} contract without a
 * runtime ClassCastException.
 */
@ApiStatus.Experimental
public class KeyValueEvent<S, V> {

    public @Nullable S scope;
    public @Nullable String scopeKey;
    public @Nullable String key;
    public @Nullable V value;
    public long updatedEpochMs;

    /** Set to {@code true} to issue a remove instead of a put. */
    public boolean remove;

    public void clear() {
        scope = null;
        scopeKey = null;
        key = null;
        value = null;
        updatedEpochMs = 0L;
        remove = false;
    }
}
