package ac.grim.grimac.api.checks;

import ac.grim.grimac.api.AbstractProcessor;
import ac.grim.grimac.api.common.BasicStatus;

public interface AbstractCheck extends AbstractProcessor, BasicStatus {

    String getCheckName();

    default String getAlternativeName() {
        return getCheckName();
    }

    /**
     * @return Current violation amount.
     */
    double getViolations();

    /**
     * @return The last time a violation occurred.
     */
    long getLastViolation();

    double getDecay();

    double getSetbackVL();

    /**
     * @return Is the check experimental?
     */
    boolean isExperimental();

    /**
     * Checks if violations need to be reset or decayed based on the provided time.
     */
    default void checkViolations(long time) {

    }

    /**
     * Checks if violations need to be reset or decayed based on the current time.
     */
    default void checkViolations() {
        checkViolations(System.currentTimeMillis());
    }

    /**
     * Is the check enabled and supported?
     * @return True if active
     */
    default boolean isActive() {
        return isEnabled() && isSupported();
    }

}
