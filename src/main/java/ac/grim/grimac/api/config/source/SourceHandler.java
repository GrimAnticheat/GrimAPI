package ac.grim.grimac.api.config.source;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SourceHandler<T extends ConfigSource> {
    /**
     * Handles the loading of a specific source type.
     * @param source  The configuration data object.
     * @param manager The manager instance (use loadFile, loadMap, etc).
     */
    void handle(@NotNull T source, @NotNull ConfigLoader manager);
}