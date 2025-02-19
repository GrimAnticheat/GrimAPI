package ac.grim.grimac.api.alerts;

import org.bukkit.entity.Player;

public interface AlertManager {

    /**
     * Checks if the player has alerts enabled.
     *
     * @param player
     * @return boolean
     */
    boolean hasAlertsEnabled(Player player);

    /**
     * Toggles alerts for the player.
     *
     * @param player
     */
    default void toggleAlerts(Player player) {
        toggleAlerts(player, false);
    }

    /**
     * Toggles alerts for the player.
     *
     * @param player
     * @param silent - if set to true, the player won't be notified about the action
     */
    void toggleAlerts(Player player, boolean silent);

    /**
     * Checks if the player has verbose enabled.
     *
     * @param player
     * @return boolean
     */
    boolean hasVerboseEnabled(Player player);

    /**
     * Toggles verbose for the player.
     *
     * @param player
     */
    default void toggleVerbose(Player player) {
        toggleVerbose(player, false);
    }

    /**
     * Toggles verbose for the player.
     *
     * @param player
     * @param silent - if set to true, the player won't be notified about the action
     */
    void toggleVerbose(Player player, boolean silent);

    boolean hasBrandsEnabled(Player player);

    void toggleBrands(Player player, boolean silent);
}
