package ac.grim.grimac.api.event.events;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.event.GrimEvent;

/**
 * Fired when Grim sends a teleport packet to the client.
 *
 * <p>This event exists to help maintain compatibility with packet-based plugins and
 * other anticheats that keep track of inbound / outbound teleport packets to build a deque.</p>
 *
 * <p>This event is fired on the Netty thread associated with the PacketEvents
 * user represented by the {@link GrimUser}.</p>
 */
public class GrimTeleportEvent extends GrimEvent implements GrimUserEvent {
    private final GrimUser user;
    private final int teleportId;
    private final long timestamp;

    public GrimTeleportEvent(GrimUser user, int teleportId, long timestamp) {
        super();
        this.user = user;
        this.teleportId = teleportId;
        this.timestamp = timestamp;
    }

    @Override
    public GrimUser getUser() {
        return user;
    }

    /**
     * Returns the teleport id sent to the client.
     *
     * @return the teleport id that will be echoed in the teleport acknowledgement
     */
    public int getTeleportId() {
        return teleportId;
    }

    /**
     * Returns the timestamp captured when the teleport packet was sent.
     *
     * @return the event timestamp in milliseconds
     */
    public long getTimestamp() {
        return timestamp;
    }
}
