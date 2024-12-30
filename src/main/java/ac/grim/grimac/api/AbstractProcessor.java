package ac.grim.grimac.api;

import ac.grim.grimac.api.checks.ListenerType;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;

public interface AbstractProcessor extends BasicReloadable, ConfigReloadable {
    // name of the check
    String getConfigName();
    // listener type of the check
    ListenerType getListenerType();
    // if the player supports the check
    boolean isSupported();
    // how many listeners
    int getListeners();

    void setListeners(int listeners);

}

