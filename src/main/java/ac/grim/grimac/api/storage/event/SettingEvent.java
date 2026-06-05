package ac.grim.grimac.api.storage.event;

import ac.grim.grimac.api.storage.kind.KeyValueEvent;
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
 * <p>
 * Extends {@link KeyValueEvent} so the v2 {@code MongoKeyValueScopedAdapter}
 * (and its SQL/Redis siblings) can accept SettingEvent slots without a
 * runtime ClassCastException — the KV handler reads fields directly off
 * the parent type while existing callers keep the fluent setter API.
 */
@ApiStatus.Experimental
public final class SettingEvent extends KeyValueEvent<SettingScope, byte[]> {

    public @NotNull SettingScope scope() {
        if (scope == null) throw new IllegalStateException("SettingEvent.scope not set");
        return scope;
    }
    public @NotNull SettingEvent scope(@NotNull SettingScope v) { this.scope = v; return this; }

    public @NotNull String scopeKey() {
        if (scopeKey == null) throw new IllegalStateException("SettingEvent.scopeKey not set");
        return scopeKey;
    }
    public @NotNull SettingEvent scopeKey(@NotNull String v) { this.scopeKey = v; return this; }

    public @NotNull String key() {
        if (key == null) throw new IllegalStateException("SettingEvent.key not set");
        return key;
    }
    public @NotNull SettingEvent key(@NotNull String v) { this.key = v; return this; }

    public byte @NotNull [] value() {
        if (value == null) throw new IllegalStateException("SettingEvent.value not set");
        return value;
    }
    public @NotNull SettingEvent value(byte @NotNull [] v) { this.value = v; return this; }

    public long updatedEpochMs() { return updatedEpochMs; }
    public @NotNull SettingEvent updatedEpochMs(long v) { this.updatedEpochMs = v; return this; }

    public void reset() { clear(); }
}
