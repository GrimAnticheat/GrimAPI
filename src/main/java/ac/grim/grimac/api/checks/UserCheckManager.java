package ac.grim.grimac.api.checks;


import org.jetbrains.annotations.Nullable;

import java.util.Collection;

public interface UserCheckManager {

    /**
     * Register a check
     *
     * @param clazz     Check class
     * @param check Check instance
     * @return Was successful in registering
     */
    <T extends CustomCheck> boolean addCustomCheck(Class<T> clazz, T check);

    /**
     * Register a check
     *
     * @param clazz Check class
     * @return Was successful in unregistering
     */
    <T extends CustomCheck> boolean removeCustomCheck(Class<T> clazz);

    /**
     * Get a check by its class
     * @param clazz Check class
     * @return Check instance
     */
    @Nullable <T extends AbstractCheck> T getCheckByClass(Class<T> clazz);

    /**
     * Get a check by its name
     * @param name Check name
     * @return Check instance
     */
    @Nullable AbstractCheck getCheckByName(String name);

    /**
     * Get all checks
     * @return All checks
     */
    Collection<AbstractCheck> getAllChecks();

    /**
     * Handle a violation for a check
     * @param check   Check instance
     * @param verbose Verbose message
     * @return If the violation was handled
     */
    <T extends CustomCheck> boolean onCustomCheckViolation(T check, String verbose);

}
