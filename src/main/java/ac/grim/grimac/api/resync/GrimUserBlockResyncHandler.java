package ac.grim.grimac.api.resync;


public interface GrimUserBlockResyncHandler {

    /**
     * Resynchronizes the user's block data within the specified block region (fixes "ghost" blocks).
     *
     * @param minBlockX The minimum X coordinate of the block region to resynchronize.
     * @param minBlockY The minimum Y coordinate of the block region to resynchronize.
     * @param minBlockZ The minimum Z coordinate of the block region to resynchronize.
     * @param maxBlockX The maximum X coordinate of the block region to resynchronize.
     * @param maxBlockY The maximum Y coordinate of the block region to resynchronize.
     * @param maxBlockZ The maximum Z coordinate of the block region to resynchronize.
     */
    void resync(int minBlockX, int minBlockY, int minBlockZ, int maxBlockX, int maxBlockY, int maxBlockZ);
}