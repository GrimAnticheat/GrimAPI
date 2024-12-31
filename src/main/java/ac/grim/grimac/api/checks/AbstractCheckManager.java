package ac.grim.grimac.api.checks;


import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.AbstractProcessor;
import ac.grim.grimac.api.GrimUser;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

public interface AbstractCheckManager {

    /**
     * Register a processor
     * @param clazz Processor class
     * @param processor instance
     * @return How many listeners were registered for the processor
     */
    <T extends AbstractProcessor> int registerProcessor(Class<T> clazz, T processor);

    /**
     * Register a processor
     * @param clazz Processor class
     * @return Was successful in unregistering
     */
    <T extends AbstractProcessor> boolean unregisterProcessor(Class<T> clazz);

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

    /**
     * Get a processor
     * @param clazz Processor class
     * @return Processor instance
     */
    @Nullable <T extends AbstractProcessor> T getProcessor(Class<T> clazz);

    /**
     * Handle a violation for a check
     * @param check Check instance
     * @param verbose Verbose message
     * @return If the violation was handled
     */
    boolean handleViolation(GrimUser user, AbstractCheck check, String verbose);

}
