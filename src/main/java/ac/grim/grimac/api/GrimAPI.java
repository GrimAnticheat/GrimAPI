package ac.grim.grimac.api;

import ac.grim.grimac.api.alerts.AlertManager;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigManager;
import ac.grim.grimac.api.config.ConfigReloadable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface GrimAPI<USER> extends ConfigReloadable, BasicReloadable {
    static <USER> GrimAPI<USER> getAPI() {
        return GrimPlatform.<USER>getInstance().getApi();
    }

    /**
     * Reloads Grim using the config file.
     */
    @Override
    default void reload() {
        this.reload(this.getConfigManager());
    }

    /**
     * Reloads Grim asynchronously using the config file.
     * @return CompletableFuture<Boolean>
     */
    default CompletableFuture<Boolean> reloadAsync() {
        return this.reloadAsync(this.getConfigManager());
    }

    /**
     * Retrieves a GrimUser reference from the player.
     * @param player
     * @return GrimUser
     */
    @Nullable
    GrimUser<USER> getGrimUser(USER player);

    /**
     * This is specifically for setting the server's name in grim's discord messages.
     * Use {@link GrimAPI#registerVariable(String, String)} instead.
     * @param name
     */
    @Deprecated
    void setServerName(String name);

    /**
     * Used to create or replace variables, such as %player%. This only works
     * for player related messages.
     * @param variable
     * @param replacement
     */
    void registerVariable(String variable, @Nullable Function<GrimUser, String> replacement);

    /**
     * Used to create or replace static variables, such as %server%.
     * @param variable
     * @param replacement
     */
    void registerVariable(String variable, @Nullable String replacement);

    String getGrimVersion();

    /**
     * Used for future expansion. Don't use this unless you know what you're doing.
     */
    void registerFunction(String key, @Nullable Function<Object, Object> function);

    /**
     * Used for future expansion. Don't use this unless you know what you're doing.
     */
    @Nullable Function<Object, Object> getFunction(String key);

    /**
     * Retrieves the alert manager.
     * @return AlertManager
     */
    AlertManager<USER> getAlertManager();

    /**
     * Retrieves the config manager.
     * @return Configurable
     */
    ConfigManager getConfigManager();

}
