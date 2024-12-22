package ac.grim.grimac.api;

/**
 * Represents the types of events a check can listen to.
 * Checks can listen to multiple event types simultaneously via bitmasks.
 * For example, a check could listen to both POSITION and ROTATION events
 * by combining their respective masks.
 */
public enum CheckType {
    /**
     * For checks without any event listeners
     */
    NONE(0),

    /**
     * Packet analysis events
     */
    PACKET(1),

    /**
     * Player movement events
     */
    POSITION(2),

    /**
     * Player rotation events
     */
    ROTATION(3),

    /**
     * Vehicle-related events
     */
    VEHICLE(4),

    /**
     * Pre-prediction calculation events
     */
    PRE_PREDICTION(5),

    /**
     * Block breaking events
     */
    BLOCK_BREAK(6),

    /**
     * Block placement events
     */
    BLOCK_PLACE(7),

    /**
     * Post-prediction calculation events
     */
    POST_PREDICTION(8);

    private final int mask;

    CheckType(int bitPosition) {
        if (bitPosition > 0 && bitPosition >= 32) {
            throw new IllegalArgumentException("Cannot have more than 32 check types");
        }
        this.mask = bitPosition == 0 ? 0 : 1 << (bitPosition - 1);
    }

    public int getMask() {
        return mask;
    }
}