package ac.grim.grimac.api;

import org.jetbrains.annotations.NotNull;

/**
 * State used to initialise Grim when a player is transferred between servers.
 *
 * <p>Coordinates are represented as primitives so this API has no dependency
 * on a platform vector type.</p>
 */
public final class TransferData {
    private final double positionX;
    private final double positionY;
    private final double positionZ;
    private final double velocityX;
    private final double velocityY;
    private final double velocityZ;
    private final float yaw;
    private final float pitch;
    private final boolean hasRotation;

    private TransferData(double positionX, double positionY, double positionZ,
                         double velocityX, double velocityY, double velocityZ,
                         float yaw, float pitch, boolean hasRotation) {
        this.positionX = positionX;
        this.positionY = positionY;
        this.positionZ = positionZ;
        this.velocityX = velocityX;
        this.velocityY = velocityY;
        this.velocityZ = velocityZ;
        this.yaw = yaw;
        this.pitch = pitch;
        this.hasRotation = hasRotation;
    }

    public static @NotNull Builder builder() {
        return new Builder();
    }

    public double positionX() { return positionX; }
    public double positionY() { return positionY; }
    public double positionZ() { return positionZ; }
    public double velocityX() { return velocityX; }
    public double velocityY() { return velocityY; }
    public double velocityZ() { return velocityZ; }

    public boolean hasRotation() {
        return hasRotation;
    }

    public float yaw() {
        return yaw;
    }

    public float pitch() {
        return pitch;
    }

    public static final class Builder {
        private double positionX;
        private double positionY;
        private double positionZ;
        private double velocityX;
        private double velocityY;
        private double velocityZ;
        private boolean hasPosition;
        private boolean hasVelocity;
        private float yaw;
        private float pitch;
        private boolean hasRotation;

        public @NotNull Builder position(double x, double y, double z) {
            this.positionX = x;
            this.positionY = y;
            this.positionZ = z;
            this.hasPosition = true;
            return this;
        }

        public @NotNull Builder velocity(double x, double y, double z) {
            this.velocityX = x;
            this.velocityY = y;
            this.velocityZ = z;
            this.hasVelocity = true;
            return this;
        }

        /** Sets yaw and pitch in Minecraft's usual degree units. */
        public @NotNull Builder rotation(float yaw, float pitch) {
            this.yaw = yaw;
            this.pitch = pitch;
            this.hasRotation = true;
            return this;
        }

        public @NotNull TransferData build() {
            if (!hasPosition || !hasVelocity) {
                throw new IllegalStateException("TransferData requires both position and velocity");
            }
            return new TransferData(positionX, positionY, positionZ,
                    velocityX, velocityY, velocityZ, yaw, pitch, hasRotation);
        }
    }
}
