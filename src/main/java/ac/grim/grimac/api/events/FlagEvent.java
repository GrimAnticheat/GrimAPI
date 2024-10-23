package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class FlagEvent implements GrimCancellableEvent {
    private final GrimUser grimUser;
    private final AbstractCheck check;
    private boolean cancelled;

    public FlagEvent(GrimUser grimUser, AbstractCheck check) {
        this.grimUser = grimUser;
        this.check = check;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancel) {
        cancelled = cancel;
    }

    public GrimUser<?> getUser() {
        return grimUser;
    }

    public AbstractCheck getCheck() {
        return check;
    }

    public double getViolations() {
        return check.getViolations();
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }
}
