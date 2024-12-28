package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import org.bukkit.event.Cancellable;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class FlagEvent extends Event implements Cancellable {
    private static final HandlerList handlers = new HandlerList();
    private final GrimUser grimUser;
    private final AbstractCheck check;
    private final String verbose;
    private boolean cancelled;

    public FlagEvent(GrimUser grimUser, AbstractCheck check, String verbose) {
        super(true); // Async!
        this.grimUser = grimUser;
        this.check = check;
        this.verbose = verbose;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancel) {
        cancelled = cancel;
    }

    public GrimUser getPlayer() {
        return grimUser;
    }

    public AbstractCheck getCheck() {
        return check;
    }

    public AbstractCheck getVerbose() {
        return verbose;
    }

    public double getViolations() {
        return check.getViolations();
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }

    public boolean isSetback() {
        return check.getViolations() > check.getSetbackVL();
    }


}
