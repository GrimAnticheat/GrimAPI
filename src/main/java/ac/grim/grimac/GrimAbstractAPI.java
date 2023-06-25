package ac.grim.grimac;

import net.kyori.adventure.text.Component;
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

    /**
     * Reloads grim
     */
    void reload();

}
