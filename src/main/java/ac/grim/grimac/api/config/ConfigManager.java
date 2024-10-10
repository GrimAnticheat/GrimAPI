package ac.grim.grimac.api.config;

import ac.grim.grimac.api.common.BasicReloadable;

import java.util.List;

public interface ConfigManager extends BasicReloadable {

    String getStringElse(String key, String otherwise);

    List<String> getStringList(String key);

    List<String> getStringListElse(String key, List<String> otherwise);

    int getIntElse(String key, int otherwise);

    long getLongElse(String key, long otherwise);

    double getDoubleElse(String key, double otherwise);

    boolean getBooleanElse(String key, boolean otherwise);

    public <T> T get(String key);

}
