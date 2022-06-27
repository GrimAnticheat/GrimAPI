package ac.grim.grimac.events;

import ac.grim.grimac.AbstractCheck;
import ac.grim.grimac.GrimUser;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class CommandExecuteEvent extends FlagEvent {

    private static final HandlerList handlers = new HandlerList();
    private final String command;

    public CommandExecuteEvent(GrimUser player, AbstractCheck check, String command) {
        super(player, check); // Async!
        this.command = command;
    }

    public static HandlerList getHandlerList() {
        return handlers;
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {
        return handlers;
    }

    public String getCommand() {
        return command;
    }

}
