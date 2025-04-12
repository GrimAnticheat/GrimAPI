package ac.grim.grimac.api.alerts;

import org.bukkit.entity.Player;

public interface AlertManager {

    /**
     * Checks if the player has alerts enabled.
     * @param player
     * @return boolean
     */
    boolean hasAlertsEnabled(Player player);

    /**
     * Toggles alerts for the player.
     * @param player
     */
    void toggleAlerts(Player player);

    /**
     * Toggles alerts for the player.
     * @param player
     * @param enabled
     */
    void toggleAlerts(Player player, boolean enabled);

    /**
     * Checks if the player has verbose enabled.
     * @param player
     * @return boolean
     */
    boolean hasVerboseEnabled(Player player);

    /**
     * Toggles verbose for the player.
     * @param player
     */
    void toggleVerbose(Player player);

    /**
     * Toggles verbose for the player.
     * @param player
     * @param enabled
     */
    void toggleVerbose(Player player, boolean enabled);
}
