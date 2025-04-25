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
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to toggle alerts for
     * @return true if alerts are now enabled, false if alerts are now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleAlerts(@NonNull GrimUser player) {
        return toggleAlerts(player, false);
    }

    /**
     * Toggles alerts for the player silently or with a message.
     *
     * @param player The GrimUser to toggle alerts for
     * @param silent true to suppress any messages to the player, false to notify
     * @return true if alerts are now enabled, false if alerts are now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleAlerts(@NonNull GrimUser player, boolean silent) {
        boolean newState = !hasAlertsEnabled(player);
        setAlertsEnabled(player, newState, silent);
        return newState;
    }

    /**
     * Sets the alert state for the player.
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to set alerts for
     * @param enabled true to enable alerts, false to disable
     * @throws NullPointerException if player is null
     */
    default void setAlertsEnabled(@NonNull GrimUser player, boolean enabled) {
        setAlertsEnabled(player, enabled, false);
    }

    /**
     * Sets the alert state for the player silently or with a message.
     *
     * @param player The GrimUser to set alerts for
     * @param enabled true to enable alerts, false to disable
     * @param silent true to suppress any messages to the player, false to notify
     * @throws NullPointerException if player is null
     */
    void setAlertsEnabled(@NonNull GrimUser player, boolean enabled, boolean silent);

    // ------------------- VERBOSE -------------------

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
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to toggle verbose for
     * @return true if verbose is now enabled, false if verbose is now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleVerbose(@NonNull GrimUser player) {
        return toggleVerbose(player, false);
    }

    /**
     * Toggles verbose for the player silently or with a message.
     *
     * @param player The GrimUser to toggle verbose for
     * @param silent true to suppress any messages to the player, false to notify
     * @return true if verbose is now enabled, false if verbose is now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleVerbose(@NonNull GrimUser player, boolean silent) {
        boolean newState = !hasVerboseEnabled(player);
        setVerboseEnabled(player, newState, silent);
        return newState;
    }

    /**
     * Sets the verbose state for the player.
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to set verbose for
     * @param enabled true to enable verbose, false to disable
     * @throws NullPointerException if player is null
     */
    default void setVerboseEnabled(@NonNull GrimUser player, boolean enabled) {
        setVerboseEnabled(player, enabled, false);
    }

    /**
     * Sets the verbose state for the player silently or with a message.
     *
     * @param player The GrimUser to set verbose for
     * @param enabled true to enable verbose, false to disable
     * @param silent true to suppress any messages to the player, false to notify
     * @throws NullPointerException if player is null
     */
    void setVerboseEnabled(@NonNull GrimUser player, boolean enabled, boolean silent);

    // ------------------- BRANDS -------------------

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
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to toggle brand notifications for
     * @return true if brand notifications are now enabled, false if brand notifications are now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleBrands(@NonNull GrimUser player) {
        return toggleBrands(player, false);
    }

    /**
     * Toggles brand notifications for the player silently or with a message.
     *
     * @param player The GrimUser to toggle brand notifications for
     * @param silent true to suppress any messages to the player, false to notify
     * @return true if brand notifications are now enabled, false if brand notifications are now disabled
     * @throws NullPointerException if player is null
     */
    default boolean toggleBrands(@NonNull GrimUser player, boolean silent) {
        boolean newState = !hasBrandsEnabled(player);
        setBrandsEnabled(player, newState, silent);
        return newState;
    }

    /**
     * Sets the brand notification state for the player.
     * Sends a message to the player indicating the new state.
     *
     * @param player The GrimUser to set brand notifications for
     * @param enabled true to enable brand notifications, false to disable
     * @throws NullPointerException if player is null
     */
    default void setBrandsEnabled(@NonNull GrimUser player, boolean enabled) {
        setBrandsEnabled(player, enabled, false);
    }

    /**
     * Sets the brand notification state for the player silently or with a message.
     *
     * @param player The GrimUser to set brand notifications for
     * @param enabled true to enable brand notifications, false to disable
     * @param silent true to suppress any messages to the player, false to notify
     * @throws NullPointerException if player is null
     */
    void setBrandsEnabled(@NonNull GrimUser player, boolean enabled, boolean silent);
}