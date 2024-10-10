package ac.grim.grimac.api.events;

import lombok.Getter;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class GrimReloadEvent extends Event {

    private static final HandlerList handlers = new HandlerList();

    @Getter private final boolean success;

    public GrimReloadEvent(boolean success) {
        super(true); // Async!
        this.success = success;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

}
