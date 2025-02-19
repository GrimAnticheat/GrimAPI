package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;

public class GrimQuitEvent extends GrimEvent implements GrimUserEvent {
    private final GrimUser user;

    public GrimQuitEvent(GrimUser user) {
        super(true); // Async
        this.user = user;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }
}
