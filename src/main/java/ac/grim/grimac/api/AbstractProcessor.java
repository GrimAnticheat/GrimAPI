package ac.grim.grimac.api;

import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;

public interface AbstractProcessor extends ConfigReloadable, BasicReloadable {

    String getConfigName();

}

