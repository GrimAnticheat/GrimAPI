package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicStatus;

public interface AbstractCheck extends AbstractProcessor, BasicStatus {

    String getCheckName();

    default String getAlternativeName() {
        return getCheckName();
    }

    double getViolations();

    double getDecay();

    double getSetbackVL();

    boolean isExperimental();

    /**
     * Does the check support the player.
     */
    boolean supportsPlayer(GrimUser user);

    /**
     * Is the check active.
     */
    default boolean isActiveCheck() {
        return isSupported() && isEnabled();
    }

    /**
     * Checks if violations need to be reset or decayed based on the provided time.
     */
    void checkViolations(long time);

    /**
     * Checks if violations need to be reset or decayed based on the current time.
     */
    default void checkViolations() {
        checkViolations(System.currentTimeMillis());
    }

}
