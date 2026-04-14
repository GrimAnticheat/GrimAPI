package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;

/**
 * Fired when Grim sends a transaction packet to the client.
 *
 * <p>Plugins can use this event to track transaction ids issued by Grim and
 * correlate them with the matching {@link GrimTransactionReceivedEvent} once a
 * response is received.</p>
 *
 * <p>This event is fired on the Netty thread associated with the user
 * represented by the {@link GrimUser}.</p>
 */
public class GrimTransactionSendEvent extends GrimEvent implements GrimUserEvent {
    private final GrimUser user;
    private final int transactionId;
    private final long timestamp;

    public GrimTransactionSendEvent(GrimUser user, int transactionId, long timestamp) {
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
     * Returns the id of the transaction packet sent to the client.
     *
     * @return the outbound transaction id
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Returns the timestamp captured when the transaction packet was sent.
     *
     * @return the event timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }
}
