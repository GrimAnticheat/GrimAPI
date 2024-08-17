package ac.grim.grimac.api;

import ac.grim.grimac.api.alerts.AlertManager;
import org.bukkit.entity.Player;

import javax.annotation.Nullable;
import java.util.function.Function;

public interface GrimAbstractAPI {

    /**
     * Retrieves a GrimUser reference from the player.
     * @param player
     * @return GrimUser
     */
    @Nullable
    GrimUser getGrimUser(Player player);

    /**
     * This is specifically for setting the server's name in grim's discord messages.
     * Use {@link GrimAbstractAPI#registerVariable(String, String)} instead.
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
    void registerVariable(String variable, Function<GrimUser, String> replacement);

    /**
     * Used to create or replace static variables, such as %server%.
     * @param variable
     * @param replacement
     */
    void registerVariable(String variable, String replacement);

    String getGrimVersion();

    /**
     * Used for future expansion. Don't use this unless you know what you're doing.
     */
    void registerFunction(String key, Function<Object, Object> function);

    /**
     * Used for future expansion. Don't use this unless you know what you're doing.
     */
    Function<Object, Object> getFunction(String key);

    /**
     * Reloads grim
     */
    void reload();

    /**
     * Retrieves the alert manager.
     * @return AlertManager
     */
    AlertManager getAlertManager();
}
