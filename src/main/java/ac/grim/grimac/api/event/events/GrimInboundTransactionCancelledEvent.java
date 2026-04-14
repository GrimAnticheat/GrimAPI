package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;

/**
 * Fired when Grim cancels an inbound transaction packet.
 *
 * <p>This event exists to help maintain compatibility with packet-based plugins and
 * other anticheats that keep track of inbound / outbound transaction packets to build a deque.</p>
 *
 * <p>This event is not fired when `disable-pong-cancelling` is enabled in the Grim
 * configuration.</p>
 *
 * <p>This event is fired on the Netty thread associated with the PacketEvents
 * user represented by the {@link GrimUser}.</p>
 */
public class GrimInboundTransactionCancelledEvent extends GrimEvent implements GrimUserEvent {
    private final GrimUser user;
    private final int transactionId;
    private final long timestamp;

    public GrimInboundTransactionCancelledEvent(GrimUser user, int transactionId, long timestamp) {
        super();
        this.user = user;
        this.transactionId = transactionId;
        this.timestamp = timestamp;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    /**
     * Returns the id of the cancelled inbound transaction.
     *
     * @return the transaction id
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Returns the timestamp captured when the transaction was cancelled.
     *
     * @return the cancellation timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }
}
