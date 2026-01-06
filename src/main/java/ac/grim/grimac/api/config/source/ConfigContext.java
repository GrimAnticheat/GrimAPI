package ac.grim.grimac.api.config.source;

import org.jetbrains.annotations.NotNull;
import java.io.File;
import java.util.Map;

/**
 * Represents the context in which a configuration source loads itself.
 * <p>
 * This interface abstracts the underlying configuration library (e.g. Configuralize),
 * allowing sources to register data without knowing how it is stored.
 */
public interface ConfigContext {

    /**
     * Registers a file-based configuration source.
     *
     * @param id            The unique identifier for this source.
     * @param file          The file on disk.
     * @param resourceOwner The class used to locate the default resource in the JAR.
     */
    void addFileSource(@NotNull String id, @NotNull File file, @NotNull Class<?> resourceOwner);

    /**
     * Registers a map-based configuration source.
     *
     * @param id     The unique identifier for this source.
     * @param values The raw values to load.
     */
    void addMapSource(@NotNull String id, @NotNull Map<String, Object> values);

}