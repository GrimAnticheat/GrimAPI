package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.Cancellable;
import ac.grim.grimac.api.event.GrimEvent;

import java.util.HashMap;

public class FlagEvent extends GrimVerboseCheckEvent {

    public FlagEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check, verbose);
    }
}
