package ac.grim.grimac.api;

public interface PacketWorld {
    /**
     * @param x x-coordinate of the block
     * @param y y-coordinate of the block
     * @param z z-coordinate of the block
     * @return the block state ID at the given coordinates, or a negative error code:
     * <ul>
     *   <li>-1 if the player's world does not support negative Y values and {@code y < 0}</li>
     *   <li>-2 if the chunk column is missing or the Y value is outside the loaded height range</li>
     *   <li>-3 if an unexpected error occurs while resolving the block</li>
     * </ul>
     */
    int getBlockStateId(int x, int y, int z);
}
