package ac.grim.grimac;
import org.bukkit.entity.Player;

import javax.annotation.Nullable;

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
     * @param name
     */
    void setServerName(String name);
}
