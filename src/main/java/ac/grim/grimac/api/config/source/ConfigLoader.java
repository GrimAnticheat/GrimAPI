package ac.grim.grimac.api.config.source;

import org.jetbrains.annotations.NotNull;
import java.io.File;
import java.util.Map;

/**
 * Exposes the primitive methods to inject configuration data into Grim.
 * Passed to SourceHandlers.
 */
public interface ConfigLoader {

    /**
     * Injects a standard file-based source.
     * The system will handle parsing (YAML/JSON) based on extension.
     */
    void loadFile(@NotNull String id, @NotNull File file, @NotNull Class<?> resourceOwner);

    /**
     * Injects raw key-value pairs.
     * Use this for Redis, SQL, S3 (parsed), or HTTP sources.
     *
     * @param id The scope ID.
     * @param values The map of flattened keys (e.g. "checks.killaura.enabled") to values.
     */
    void loadMap(@NotNull String id, @NotNull Map<String, Object> values);
}