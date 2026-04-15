package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;

/**
 * Fired when Grim receives an inbound response for a transaction packet that it
 * previously sent to the client.
 *
 * <p>Grim cancels these inbound packets by default. This behavior is
 * controlled by the {@code disable-pong-cancelling} option in
 * {@code config.yml}.</p>
 *
 * <p>This event only fires for transaction packets initiated by Grim.</p>
 *
 * <p>This event is fired on the Netty thread associated with the user
 * represented by the {@link GrimUser}.</p>
 */
public class GrimTransactionReceivedEvent extends GrimEvent implements GrimUserEvent {
    private final GrimUser user;
    private final int transactionId;
    private final boolean packetCancelled;
    private final long timestamp;

    public GrimTransactionReceivedEvent(GrimUser user, int transactionId, boolean packetCancelled, long timestamp) {
        super();
        this.user = user;
        this.transactionId = transactionId;
        this.packetCancelled = packetCancelled;
        this.timestamp = timestamp;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    /**
     * Returns the id of the transaction packet this inbound response belongs to.
     *
     * @return the transaction id echoed back by the client
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Returns whether Grim cancelled handling of the inbound transaction packet.
     *
     * <p>This flag describes packet handling, not event cancellation.</p>
     *
     * @return {@code true} if Grim cancelled the inbound packet
     */
    public boolean isPacketCancelled() {
        return packetCancelled;
    }

    /**
     * Returns the timestamp captured when the inbound transaction packet was received.
     *
     * @return the event timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }
}
