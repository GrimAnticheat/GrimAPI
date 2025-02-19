package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.event.GrimEvent;

public class GrimReloadEvent extends GrimEvent {
    private final boolean success;

    public GrimReloadEvent(boolean success) {
        super(true); // Async
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}
