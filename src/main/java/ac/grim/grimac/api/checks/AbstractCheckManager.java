package ac.grim.grimac.api.checks;


import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.AbstractProcessor;
import ac.grim.grimac.api.GrimUser;

import java.util.Collection;

public interface AbstractCheckManager {

    /**
     * Register a processor
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


    boolean handleViolation(GrimUser user, AbstractCheck check, String verbose);


}
