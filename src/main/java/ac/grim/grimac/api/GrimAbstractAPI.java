package ac.grim.grimac.api;

import ac.grim.grimac.api.alerts.AlertManager;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigManager;
import ac.grim.grimac.api.config.ConfigReloadable;
import ac.grim.grimac.api.event.EventBus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface GrimAbstractAPI extends ConfigReloadable, BasicReloadable {
    /**
     * Returns EventBus instanced used to register events and listen to Grim events
     * @return {@link EventBus}
     */
    @NotNull EventBus getEventBus();

    /**
     * Retrieves a GrimUser reference from the player's UUID.
     * @param uuid UUID of the player
     * @return GrimUser
     */
    @Nullable GrimUser getGrimUser(UUID uuid);

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

    /**
     * Retrieves the plugin version of Grim.
     * @return Grim version
     */
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
    AlertManager getAlertManager();

    /**
     * Retrieves the config manager.
     * @return Configurable
     */
    ConfigManager getConfigManager();

    /**
     * Reloads Grim using the config file.
     */
    @Override
    default void reload() {
        reload(getConfigManager());
    }

    /**
     * Reloads Grim asynchronously using the config file.
     * @return CompletableFuture<Boolean>
     */
    default CompletableFuture<Boolean> reloadAsync() {
        return reloadAsync(getConfigManager());
    }

    /**
     * Checks if the API has reached the start phase of the plugin.
     * @return boolean
     */
    boolean hasStarted();

    /**
     * Retrieves the current tick of the server.
     * @return int
     */
    int getCurrentTick();

}
