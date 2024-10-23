package ac.grim.grimac.api.events;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;

public class CompletePredictionEvent extends FlagEvent {
    private final double offset;

    public CompletePredictionEvent(GrimUser<?> grimUser, AbstractCheck check, double offset) {
        super(grimUser, check);
        this.offset = offset;
    }

    public double getOffset() {
        return this.offset;
    }
}
