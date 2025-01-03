package ac.grim.grimac.api.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.checks.AbstractCheck;
import lombok.Getter;
import org.bukkit.event.Cancellable;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class FlagEvent extends Event implements Cancellable {
    private static final HandlerList handlers = new HandlerList();
    @Getter private final GrimUser player;
    @Getter private final AbstractCheck check;
    @Getter private final String verbose;
    private boolean cancelled;

    public FlagEvent(GrimUser player, AbstractCheck check, String verbose) {
        super(true); // Async!
        this.player = player;
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
