package ac.grim.grimac.api.config;

import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.source.ConfigSource;
import ac.grim.grimac.api.config.source.SourceHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public interface ConfigManager extends BasicReloadable {

    /**
     * Registers a configuration source.
     * The Manager looks up the registered Handler for this source type and executes it.
     */
    void registerSource(@NotNull ConfigSource source);

    /**
     * Registers a handler logic for a specific ConfigSource type.
     * Example: Registering logic for how to handle Redis sources.
     */
    <T extends ConfigSource> void registerHandler(@NotNull Class<T> type, @NotNull SourceHandler<T> handler);

    String getStringElse(String key, String otherwise);

    @Nullable String getString(String key);

    @Nullable List<String> getStringList(String key);

    List<String> getStringListElse(String key, List<String> otherwise);

    int getIntElse(String key, int otherwise);

    long getLongElse(String key, long otherwise);

    double getDoubleElse(String key, double otherwise);

    boolean getBooleanElse(String key, boolean otherwise);

    @Nullable <T> T get(String key);

    @Nullable <T> T getElse(String key, T otherwise);

    @Nullable <K, V> Map<K, V> getMap(String key);

    @Nullable <K, V> Map<K, V> getMapElse(String key, Map<K, V> otherwise);

    @Nullable <T> List<T> getList(String key);

    @Nullable <T> List<T> getListElse(String key, List<T> otherwise);

    boolean hasLoaded();

}
