package ac.grim.grimac.api;

import ac.grim.grimac.api.checks.ListenerType;
import ac.grim.grimac.api.common.BasicReloadable;

public interface AbstractProcessor extends BasicReloadable {

    String getConfigName();

    ListenerType getListenerType();

}

