package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class CommandExecuteEvent extends FlagEvent {
    private final AbstractCheck check;
    private final String command;

    public CommandExecuteEvent(GrimUser<?> player, AbstractCheck check, String command) {
        super(player, check);
        this.check = check;
        this.command = command;
    }

    public AbstractCheck getCheck() {
        return this.check;
    }

    public String getCommand() {
        return this.command;
    }

}
