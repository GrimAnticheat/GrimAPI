package ac.grim.grimac.api.config;

public interface ChangeableConfig extends ConfigManager {

    void set(String key, Object value);

}
