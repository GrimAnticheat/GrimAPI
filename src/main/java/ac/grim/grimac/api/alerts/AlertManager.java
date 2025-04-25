package ac.grim.grimac.api.alerts;

import ac.grim.grimac.api.GrimUser;
import lombok.NonNull;

public interface AlertManager {

    /**
     * Checks if the player has alerts enabled.
     *
     * @param player The GrimUser to check
     * @return true if the player has alerts enabled, false otherwise
     * @throws NullPointerException if player is null
     */
    boolean hasAlertsEnabled(@NonNull GrimUser player);

    /**
     * Toggles alerts for the player.
     * If alerts are currently disabled, they will be enabled.
     * If alerts are currently enabled, they will be disabled.
     *
     * @param player The GrimUser to toggle alerts for
     * @return true if alerts are now enabled, false if alerts are now disabled
     * @throws NullPointerException if player is null
     */
    boolean toggleAlerts(@NonNull GrimUser player);

    /**
     * Checks if the player has verbose enabled.
     *
     * @param player The GrimUser to check
     * @return true if the player has verbose enabled, false otherwise
     * @throws NullPointerException if player is null
     */
    boolean hasVerboseEnabled(@NonNull GrimUser player);

    /**
     * Toggles verbose for the player.
     * If verbose is currently disabled, it will be enabled.
     * If verbose is currently enabled, it will be disabled.
     *
     * @param player The GrimUser to toggle verbose for
     * @return true if verbose is now enabled, false if verbose is now disabled
     * @throws NullPointerException if player is null
     */
    boolean toggleVerbose(@NonNull GrimUser player);

    /**
     * Checks if the player has brand notifications enabled.
     *
     * @param player The GrimUser to check
     * @return true if the player has brand notifications enabled and has the "grim.brand" permission,
     *         false otherwise
     * @throws NullPointerException if player is null
     */
    boolean hasBrandsEnabled(@NonNull GrimUser player);

    /**
     * Toggles brand notifications for the player.
     * If brand notifications are currently disabled, they will be enabled.
     * If brand notifications are currently enabled, they will be disabled.
     *
     * @param player The GrimUser to toggle brand notifications for
     * @return true if brand notifications are now enabled, false if brand notifications are now disabled
     * @throws NullPointerException if player is null
     */
    boolean toggleBrands(@NonNull GrimUser player);
}
