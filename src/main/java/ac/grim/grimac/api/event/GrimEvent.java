package ac.grim.grimac.api.event;

import lombok.Setter;

public abstract class GrimEvent {
    private boolean cancelled = false;
    private final boolean async;

    protected GrimEvent() {
        this(false); // Default to sync
    }

    protected GrimEvent(boolean async) {
        this.async = async;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        if (!isCancellable()) {
            throw new IllegalStateException("Event " + getEventName() + " is not cancellable");
        }
        this.cancelled = cancelled;
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isCancellable() {
        return false; // Override in cancellable events
    }

    public String getEventName() {
        return getClass().getSimpleName();
    }
}