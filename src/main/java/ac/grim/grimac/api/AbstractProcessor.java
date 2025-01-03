package ac.grim.grimac.api;

import ac.grim.grimac.api.config.ConfigReloadable;

import java.util.Set;

public interface AbstractProcessor extends ConfigReloadable {

    /**
     * Get the name of the processor
     *
     * @return Name
     */
    String getConfigName();

    /**
     * Get the listener group of the processor
     *
     * @return Listener group
     */
    String getListenerGroup();

    /**
     * Get the priority of the processor
     *
     * @return Priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Get the dependencies of the processor
     *
     * @return Dependencies
     */
    default Set<String> dependsOn() {
        return Set.of();
    }

    /**
     * Check if the processor is supported, this is a cached value
     * @return True if supported
     */
    boolean isSupported();

}

