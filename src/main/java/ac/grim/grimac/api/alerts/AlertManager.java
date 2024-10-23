package ac.grim.grimac.api.alerts;

import ac.grim.grimac.api.GrimUser;

public interface AlertManager<PLAYER> {
    /**
     * Checks if the player has alerts enabled.
     * @param player
     * @return boolean
     */
    boolean hasAlertsEnabled(GrimUser<PLAYER> player);

    /**
     * Toggles alerts for the player.
     * @param player
     */
    void toggleAlerts(GrimUser<PLAYER> player);

    /**
     * Checks if the player has verbose enabled.
     * @param player
     * @return boolean
     */
    boolean hasVerboseEnabled(GrimUser<PLAYER> player);

    /**
     * Toggles verbose for the player.
     * @param player
     */
    void toggleVerbose(GrimUser<PLAYER> player);

}
