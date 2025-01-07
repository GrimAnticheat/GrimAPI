package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import lombok.Getter;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class CompletePredictionEvent extends FlagEvent {

    private static final HandlerList handlers = new HandlerList();
    @Getter private final double offset;
    private boolean cancelled;

    public CompletePredictionEvent(GrimUser player, AbstractCheck check, String verbose, double offset) {
        super(player, check, verbose);
        this.offset = offset;
    }

    public static HandlerList getHandlerList() {
        return handlers;
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
