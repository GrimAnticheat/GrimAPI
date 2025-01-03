package ac.grim.grimac.api.alerts;

import org.bukkit.entity.Player;

import java.util.UUID;

public interface AlertManager {

    /**
     * Checks if the player has alerts enabled.
     * @param player Player instance
     * @return Enable/Disable
     */
    boolean hasAlertsEnabled(Player player);

    /**
     * Toggles alerts for the player.
     * @param player Player instance
     */
    void toggleAlerts(Player player);

    /**
     * Should the player see alerts?
     * @param player Player instance
     * @param alerts Enable/Disable
     */
    void showAlerts(Player player, boolean alerts);

    /**
     * Checks if the player has verbose enabled.
     * @param player Player instance
     * @return Enable/Disable
     */
    boolean hasVerboseEnabled(Player player);

    /**
     * Toggles verbose for the player.
     * @param player Player instance
     */
    void toggleVerbose(Player player);

    /**
     * Should the player see verbose alerts?
     * @param player Player instance
     * @param verbose Enable/Disable
     */
    void showVerbose(Player player, boolean verbose);

    // UUID based

    /**
     * Checks if the player has alerts enabled.
     * @param playerId Player's UUID
     */
    boolean hasAlertsEnabled(UUID playerId);

    /**
     * Should the player see alerts?
     * @param playerId Player's UUID
     * @param alerts Enable/Disable
     */
    void showAlerts(UUID playerId, boolean alerts);

    /**
     * Checks if the player has verbose enabled.
     * @param playerId Player's UUID
     */
    boolean hasVerboseEnabled(UUID playerId);

    /**
     * Should the player see verbose alerts?
     * @param playerId Player's UUID
     * @param verbose Enable/Disable
     */
    void showVerbose(UUID playerId, boolean verbose);


}
