package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.Cancellable;
import ac.grim.grimac.api.event.GrimEvent;

public class FlagEvent extends GrimEvent implements GrimUserEvent, Cancellable {
    private final GrimUser user;
    private final AbstractCheck check;
    private final String verbose;
    private boolean cancelled;

    public FlagEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(true); // Async
        this.user = user;
        this.check = check;
        this.verbose = verbose;
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

    public AbstractCheck getCheck() {
        return check;
    }

    public String getVerbose() {
        return verbose;
    }

    public double getViolations() {
        return check.getViolations();
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }
}
