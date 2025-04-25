package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class CompletePredictionEvent extends FlagEvent {
    private final double offset;

    public CompletePredictionEvent(GrimUser player, AbstractCheck check, String verbose, double offset) {
        super(player, check, verbose);
        this.offset = offset;
    }

    public double getOffset() {
        return offset;
    }
}
