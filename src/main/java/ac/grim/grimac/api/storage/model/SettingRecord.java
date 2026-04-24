package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Experimental
public record SettingRecord(
        SettingScope scope,
        String scopeKey,
        String key,
        byte[] value,
        long updatedEpochMs) {

    public SettingRecord {
        if (scope == null) throw new IllegalArgumentException("scope");
        if (scopeKey == null) throw new IllegalArgumentException("scopeKey");
        if (key == null) throw new IllegalArgumentException("key");
        if (value == null) throw new IllegalArgumentException("value");
    }

    public @Nullable String asString() {
        return value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
