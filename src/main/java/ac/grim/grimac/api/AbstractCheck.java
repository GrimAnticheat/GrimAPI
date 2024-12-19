package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicStatus;
import ac.grim.grimac.api.dynamic.UnloadedBehavior;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base interface for all anti-cheat checks. Checks are modular components that detect specific
 * types of cheating behavior. This interface defines the very core functionality that all checks
 * must implement.
 */
public interface AbstractCheck extends AbstractProcessor, BasicStatus {
    /**
     * Gets the name of this check, used for identification and permissions.
     * @return The check's name
     */
    String getCheckName();

    /**
     * Gets an alternative name for this check, defaults to the check name.
     * Useful for checks that may have multiple names or variants (IE AntiKB and AntiKnockback)
     * @return The alternative check name
     */
    default String getAlternativeName() {
        return getCheckName();
    }

    /**
     * Gets the current violation level for this check.
     * @return Number of violations accumulated
     */
    double getViolations();

    /**
     * Gets the rate at which violations decay over time.
     * @return The violation decay rate
     */
    double getDecay();

    /**
     * Gets the violation level at which a setback/punishment should occur.
     * @return The setback threshold
     */
    double getSetbackVL();

    /**
     * Whether this check is experimental/in testing.
     * @return true if experimental, false if stable
     */
    boolean isExperimental();

    /**
     * Defines how this check should behave when unloaded.
     * This allows checks to specify custom behavior when they are disabled
     * rather than using the default no-op behavior.
     *
     * @return The UnloadedBehavior implementation for this check
     */
    UnloadedBehavior getUnloadedBehavior();

    /**
     * @return Set of check classes that must be loaded along with this check to run
     */
    default Set<Class<? extends AbstractCheck>> getDependencies() {
        return Stream.concat(getLoadAfter().stream(), getLoadBefore().stream())
            .collect(Collectors.toSet());
    }

    /**
     * @return Set of check classes that must run after this check
     */
    default Set<Class<? extends AbstractCheck>> getLoadAfter() {
        return Collections.emptySet();
    }

    /**
     * @return Set of check classes that must run before this check
     */
    default Set<Class<? extends AbstractCheck>> getLoadBefore() {
        return Collections.emptySet();
    }

    /**
     * Called when check is being loaded
     * @return true if loaded successfully
     */
    default boolean onLoad() {
        return true;
    }

    /**
     * Called when check is being unloaded
     */
    default void onUnload() {}

    /**
     * @return Bit mask representing all check types this check implements
     */
    int getCheckMask();

    /**
     * Test if this check is of a specific type
     * @param type The check type to test for
     * @return true if this check handles the given type
     */
    default boolean isCheckType(CheckType type) {
        return (getCheckMask() & type.getMask()) != 0;
    }
}
