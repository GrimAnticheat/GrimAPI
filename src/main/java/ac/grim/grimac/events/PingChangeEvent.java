package ac.grim.grimac.events;

import ac.grim.grimac.GrimUser;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class PingChangeEvent extends Event {
    private static final HandlerList handlers = new HandlerList();
    private final long pingNano;
    private final GrimUser player;

    public PingChangeEvent(GrimUser player, long pingNano) {
        super(true);
        this.player = player;
        this.pingNano = pingNano;
    }

    public long getPingNano() {
        return pingNano;
    }

    public GrimUser getPlayer() {
        return player;
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }
}
