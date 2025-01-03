package ac.grim.grimac.api.instance;

import ac.grim.grimac.api.GrimAbstractAPI;

public abstract class GrimInstanceProvider implements GrimAbstractAPI {

    protected void registerInstance() {
        GrimProvider.register(this);
    }

}
