package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class CommandExecuteEvent extends FlagEvent {
    private static final HandlerList handlers = new HandlerList();

    private final AbstractCheck check;
    private final String command;

    public CommandExecuteEvent(GrimUser player, AbstractCheck check, String command) {
        super(player, check); // Async!
        this.check = check;
        this.command = command;
    }

    public AbstractCheck getCheck() {
        return check;
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
