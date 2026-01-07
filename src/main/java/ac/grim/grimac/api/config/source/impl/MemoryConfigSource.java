package ac.grim.grimac.api.config.source.impl;

import ac.grim.grimac.api.config.source.ConfigSource;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

@ApiStatus.Internal
public final class MemoryConfigSource implements ConfigSource {
    private final String id;
    private final Map<String, Object> values;

    public MemoryConfigSource(String id, Map<String, Object> values) {
        this.id = id;
        this.values = values;
    }

    @Override public @NotNull String getId() { return id; }
    public Map<String, Object> getValues() { return Collections.unmodifiableMap(values); }
}