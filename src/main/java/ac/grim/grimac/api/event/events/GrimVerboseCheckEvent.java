package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public abstract class GrimVerboseCheckEvent extends GrimCheckEvent {
    private final String verbose;

    public GrimVerboseCheckEvent(GrimUser user, AbstractCheck check, String verbose) {
        super(user, check);
        this.verbose = verbose;
    }

    public String getVerbose() {
        return verbose;
    }
}