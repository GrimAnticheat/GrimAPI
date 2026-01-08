package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;

public interface GrimUserEvent {
    GrimUser getUser();
    default GrimUser getPlayer() {
        return getUser();
    }
}

