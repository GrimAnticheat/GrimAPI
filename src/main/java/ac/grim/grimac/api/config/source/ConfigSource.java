package ac.grim.grimac.api.config.source;

import ac.grim.grimac.api.config.source.impl.FileConfigSource;
import ac.grim.grimac.api.config.source.impl.MemoryConfigSource;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;

/**
 * Represents a source of configuration data.
 */
public interface ConfigSource {

    @NotNull
    String getId();

    /**
     * Creates a standard file-based configuration source.
     *
     * @param id            The unique ID (matches folder in src/main/resources).
     * @param file          The file on disk.
     * @param resourceOwner The class used to find the default resource.
     * @return A new ConfigSource instance.
     */
    static ConfigSource file(@NotNull String id, @NotNull File file, @NotNull Class<?> resourceOwner) {
        return new FileConfigSource(id, file, resourceOwner);
    }

    /**
     * Creates a memory-based source (good for Redis/SQL bridges).
     *
     * @param id     The unique ID.
     * @param values The map of values (e.g. "checks.killaura.enabled" -> true).
     * @return A new ConfigSource instance.
     */
    static ConfigSource memory(@NotNull String id, @NotNull Map<String, Object> values) {
        return new MemoryConfigSource(id, values);
    }
}