package ac.grim.grimac.api.config.source;

import ac.grim.grimac.api.config.source.impl.FileConfigSource;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * Represents a source of configuration data.
 */
public interface ConfigSource {

    /**
     * @return The unique identifier for this configuration scope.
     */
    @NotNull
    String getId();

    /**
     * Loads this source into the provided context.
     * <p>
     * Internal use only. This is called by the ConfigManager.
     *
     * @param context The context provided by the ConfigManager.
     */
    @ApiStatus.Internal
    void load(@NotNull ConfigContext context);

    // --- STATIC FACTORY METHODS ---

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
}