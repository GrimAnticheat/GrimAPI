package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class CommandExecuteEvent extends GrimVerboseCheckEvent {
    private final String command;

    public CommandExecuteEvent(GrimUser player, AbstractCheck check, String verbose, String command) {
        super(player, check, verbose);
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
