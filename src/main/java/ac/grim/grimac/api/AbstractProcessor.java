package ac.grim.grimac.api;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;
import ac.grim.grimac.api.debug.Debuggable;

public interface AbstractProcessor extends BasicReloadable, ConfigReloadable, Debuggable {
    /**
     * Get the name of the processor
     * @return Config name
     */
    String getConfigName();

    /**
     * Get the listener group of the processor
     * @return Listener group
     */
    String getListenerGroup();

    /**
     * Get the priority of the processor
     * @return Priority
     */
    default int priority() {
        return 0;
    }

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

    default String identifier() {
        return getConfigName();
    }

    /**
     * Number of times the processor has been reloaded
     * @return The number of reloads
     */
    int getReloadCount();

}

