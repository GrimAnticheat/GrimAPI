package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicStatus;

public interface AbstractCheck extends AbstractProcessor, BasicStatus {

    String getCheckName();

    default String getAlternativeName() {
        return getCheckName();
    }

    default String getDescription() {
        return "No description provided";
    }

    /**
     * Canonical cross-version identity used by the history / storage layer
     * to keep rows coherent across plugin versions. Dot-separated, lower
     * snake-case (e.g. {@code "badpackets.duplicate_slot"}). Empty string
     * means the check predates the stable-key contract; the runtime falls
     * back to {@code StableKeyMapping} in that case.
     */
    default String getStableKey() {
        return "";
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
