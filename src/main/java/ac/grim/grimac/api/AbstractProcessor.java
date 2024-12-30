package ac.grim.grimac.api;

import ac.grim.grimac.api.checks.ListenerType;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;

public interface AbstractProcessor extends BasicReloadable, ConfigReloadable {
    /**
     * Get the name of the processor
     * @return
     */
    String getConfigName();

    /**
     * Get the listener type of the processor
     * @return
     */
    ListenerType getListenerType();
    /**
     * Check if the processor is supported
     * @return If the processor is supported
     */
    boolean isSupported();

    /**
     * Get the listeners of the processor
     * @return The number of listeners
     */
    int getListeners();

    /**
     * Set the listeners of the processor
     * @param listeners The number of listeners
     */
    void setListeners(int listeners);

}

