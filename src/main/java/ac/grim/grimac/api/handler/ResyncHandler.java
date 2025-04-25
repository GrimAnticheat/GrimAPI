package ac.grim.grimac.api.handler;

/**
 * A general-purpose interface for resynchronizing block data between the server and client.
 * <p>
 * This interface allows platforms and advanced server implementations to control how block updates
 * are sent to players, particularly in environments that use custom, virtual, or protocol-based worlds
 * where the backend server (e.g., Bukkit, Fabric) does not accurately reflect the client view — or
 * is not directly accessible at all.
 *
 * <p>
 * The {@code ResyncHandler} enables:
 * <ul>
 *   <li>General-purpose region resyncs (e.g., after anti-cheat corrections, rollbacks, or plugin-driven changes)</li>
 *   <li>Targeted block resyncs after cancelled block breaks flagged by Grim’s block breaking checks</li>
 *   <li>Packet-driven world updates in environments without backend world access (e.g. proxies)</li>
 * </ul>
 *
 * <p><strong>Default behavior:</strong></p>
 * Grim provides an internal fallback implementation that pulls block data from the platform's backend world
 * (e.g., Bukkit or Fabric) and sends block change packets to the client. This works for most standard setups.
 *
 * <p><strong>When to override:</strong></p>
 * You should implement and register a custom {@code ResyncHandler} if:
 * <ul>
 *   <li>Your server uses a custom or virtual world that doesn't match the backend world state</li>
 *   <li>You simulate world state via packets (e.g., instancing, replays, artificial environments)</li>
 *   <li>You are running behind a proxy or in a multi-process architecture where the backend server
 *       is not directly accessible (e.g., it's on another machine, container, or process)</li>
 * </ul>
 *
 * <p><strong>Method usage:</strong></p>
 * <ul>
 *   <li>{@link #resync} is general-purpose and may be called in any scenario requiring a region update.</li>
 *   <li>{@link #resyncPosition} is only used internally by Grim when a player fails or flags one of Grim’s
 *       block breaking checks, and their block break is cancelled. It ensures the client sees the correct
 *       block state (e.g., restores a broken block that was not allowed to be broken).</li>
 * </ul>
 */
public interface ResyncHandler {

    /**
     * Resynchronizes a rectangular region of blocks with the client.
     * <p>
     * This method may be invoked in various general-purpose scenarios, such as after rollbacks,
     * physics corrections, or plugin-driven world changes. It is the primary method for bulk updates.
     * <p>
     * Implementors should avoid triggering chunk loads and only operate on already-loaded chunks.
     *
     * @param minBlockX The minimum X coordinate of the region.
     * @param minBlockY The minimum Y coordinate of the region.
     * @param minBlockZ The minimum Z coordinate of the region.
     * @param maxBlockX The maximum X coordinate of the region.
     * @param maxBlockY The maximum Y coordinate of the region.
     * @param maxBlockZ The maximum Z coordinate of the region.
     */
    void resync(int minBlockX, int minBlockY, int minBlockZ, int maxBlockX, int maxBlockY, int maxBlockZ);

    /**
     * Resynchronizes a single block with the client after a cancelled block break.
     * <p>
     * This method is only used internally by Grim when a player attempts to break a block
     * and fails a block break check (e.g., nuker, speed mine, reach). If the block break is
     * cancelled, Grim will call this method to restore the correct block on the client and
     * prevent ghosting or visual desync.
     * <p>
     * For Minecraft 1.19+ clients, the {@code sequence} parameter is used to send an acknowledgment
     * so the client applies the change immediately.
     *
     * @param x        The X coordinate of the block.
     * @param y        The Y coordinate of the block.
     * @param z        The Z coordinate of the block.
     * @param sequence The sequence ID of the block break packet (used for 1.19+ acknowledgment).
     */
    void resyncPosition(int x, int y, int z, int sequence);
}