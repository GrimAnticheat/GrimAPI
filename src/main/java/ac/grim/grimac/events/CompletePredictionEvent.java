package ac.grim.grimac.events;

import ac.grim.grimac.AbstractCheck;
import ac.grim.grimac.GrimUser;
import org.bukkit.event.Cancellable;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class CompletePredictionEvent extends FlagEvent implements Cancellable {

    private static final HandlerList handlers = new HandlerList();
    private final double offset;
    private boolean cancelled;

    public CompletePredictionEvent(GrimUser grimUser, AbstractCheck check, double offset) {
        super(grimUser, check);
        this.offset = offset;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }

    public double getOffset() {
        return offset;
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancel) {
        cancelled = cancel;
    }

}
