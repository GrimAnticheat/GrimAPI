package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;
import ac.grim.grimac.api.event.Cancellable;

public interface GrimUserEvent {
    GrimUser getUser();
    default GrimUser getPlayer() {
        return getUser();
    }
}

