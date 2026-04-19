package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Mutable write-path slot for the {@code SETTING} category. Immutable read
 * counterpart is {@link SettingRecord}. The {@code value} byte array is borrowed
 * from the producer for the duration of {@code publish} → {@code onEvent}; the
 * consumer must snapshot it before any slow work if it intends to persist
 * asynchronously.
 */
@ApiStatus.Experimental
public final class SettingEvent {

    private SettingScope scope;
    private String scopeKey;
    private String key;
    private byte[] value;
    private long updatedEpochMs;

    public @NotNull SettingScope scope() { return scope; }
    public @NotNull SettingEvent scope(@NotNull SettingScope v) { this.scope = v; return this; }

    public @NotNull String scopeKey() { return scopeKey; }
    public @NotNull SettingEvent scopeKey(@NotNull String v) { this.scopeKey = v; return this; }

    public @NotNull String key() { return key; }
    public @NotNull SettingEvent key(@NotNull String v) { this.key = v; return this; }

    public byte @NotNull [] value() { return value; }
    public @NotNull SettingEvent value(byte @NotNull [] v) { this.value = v; return this; }

    public long updatedEpochMs() { return updatedEpochMs; }
    public @NotNull SettingEvent updatedEpochMs(long v) { this.updatedEpochMs = v; return this; }

    public void reset() {
        scope = null;
        scopeKey = null;
        key = null;
        value = null;
        updatedEpochMs = 0L;
    }
}
