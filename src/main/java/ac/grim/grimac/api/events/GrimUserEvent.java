package ac.grim.grimac.api.events;

import ac.grim.grimac.api.GrimUser;

public interface GrimUserEvent {

    GrimUser getUser();

    default GrimUser getPlayer() {
        return getUser();
    }

}
