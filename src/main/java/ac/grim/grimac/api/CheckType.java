package ac.grim.grimac.api;

/**
 * Represents different types of checks that can be performed
 */
public enum CheckType {
    PACKET(0),
    POSITION(1),
    ROTATION(2),
    VEHICLE(3),
    PRE_PREDICTION(4), 
    BLOCK_BREAK(5),
    BLOCK_PLACE(6),
    POST_PREDICTION(7);

    private final int mask;
    
    CheckType(int bitPosition) {
        if (bitPosition >= 32) {
            throw new IllegalArgumentException("Cannot have more than 32 check types");
        }
        this.mask = 1 << bitPosition;
    }
    
    public int getMask() {
        return mask;
    }
}
