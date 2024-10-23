package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;

public interface AbstractCheck extends ConfigReloadable, BasicReloadable {
    String getCheckName();

    String getAlternativeName();

    String getConfigName();

    double getViolations();

    double getDecay();

    double getSetbackVL();

    void setEnabled(boolean enabled);

    boolean isExperimental();
}
