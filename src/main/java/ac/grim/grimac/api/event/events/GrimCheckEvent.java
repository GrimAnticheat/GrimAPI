package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.Cancellable;
import ac.grim.grimac.api.event.GrimEvent;
import lombok.Getter;

public abstract class GrimCheckEvent extends GrimEvent implements GrimUserEvent, Cancellable {
    private final GrimUser user;
    @Getter
    protected final AbstractCheck check;
    private boolean cancelled;

    public GrimCheckEvent(GrimUser user, AbstractCheck check) {
        super(true); // Async
        this.user = user;
        this.check = check;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public boolean isCancellable() {
        return true;
    }

    public double getViolations() {
        return check.getViolations();
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }
}