package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class CompletePredictionEvent extends GrimCheckEvent {
    private final double offset;

    public CompletePredictionEvent(GrimUser player, AbstractCheck check, double offset) {
        super(player, check);
        this.offset = offset;
    }

    public double getOffset() {
        return offset;
    }
}
