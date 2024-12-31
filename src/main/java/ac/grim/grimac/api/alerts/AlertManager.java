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
     * Should the player see alerts?
     * @param player
     * @param alerts
     */
    void showAlerts(Player player, boolean alerts);

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
     * Should the player see verbose alerts?
     * @param player
     * @param verbose
     */
    void showVerbose(Player player, boolean verbose);

}
