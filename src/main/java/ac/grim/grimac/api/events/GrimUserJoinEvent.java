package ac.grim.grimac.api.events;

import ac.grim.grimac.api.GrimUser;
import lombok.Getter;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class GrimUserJoinEvent extends Event {

    private static final HandlerList handlers = new HandlerList();

    @Getter
    private final GrimUser user;

    public GrimUserJoinEvent(GrimUser user) {
        super(true); // Async!
        this.user = user;
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
