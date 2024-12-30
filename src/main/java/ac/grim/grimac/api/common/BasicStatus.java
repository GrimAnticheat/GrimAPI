package ac.grim.grimac.api.common;

public interface BasicStatus {

    /**
     * If the object is enabled.
     * @return If the object is enabled
     */
    boolean isEnabled();

    /**
     * Sets the object to enabled or disabled.
     */
    void setEnabled(boolean enabled);

    /**
     * Toggles the object.
     */
    default void toggle() {
        setEnabled(!isEnabled());
    }

    /**
     * If the object is disabled.
     * @return
     */
    default boolean isDisabled() {
        return !isEnabled();
    }

}
