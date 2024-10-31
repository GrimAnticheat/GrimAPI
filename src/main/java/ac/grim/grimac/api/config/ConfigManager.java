package ac.grim.grimac.api.config;

import ac.grim.grimac.api.common.BasicReloadable;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public interface ConfigManager extends BasicReloadable {

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

}
