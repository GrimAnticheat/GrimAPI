package ac.grim.grimac.api.checks;


import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.AbstractProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface AbstractCheckManager {

    /**
     * Register a processor
     * @param clazz The class of the processor
     * @param processor The processor
     * @return The number of listeners registered
     */
    <T extends AbstractProcessor> int registerProcessor(@NotNull Class<T> clazz, @NotNull T processor);

    /**
     * Unregister a processor
     * @param processor The processor
     */

    void unregisterProcessor(AbstractProcessor processor);

    /**
     * Get all processors
     * @return All processors
     */
    Collection<? extends AbstractProcessor> getAllProcessors();

    /**
     * Get all checks
     * @return All checks
     */
    Collection<? extends AbstractCheck> getAllChecks();

}
