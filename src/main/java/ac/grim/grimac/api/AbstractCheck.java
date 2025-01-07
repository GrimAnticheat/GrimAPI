package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicStatus;

public interface AbstractCheck extends AbstractProcessor, BasicStatus {

    String getCheckName();

    default String getAlternativeName() {
        return getCheckName();
    }

    double getViolations();

    /**
     * Returns the time of the last violation in UTC milliseconds or 0 if no violations have occurred.
     * Internally uses {@link System#currentTimeMillis()} when a violation occurs.
     * @return the time of the last violation in UTC milliseconds
     */
    long getLastViolationTime();

    double getDecay();

    double getSetbackVL();

    boolean isExperimental();

}
