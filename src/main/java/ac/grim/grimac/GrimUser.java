package ac.grim.grimac;

import java.util.Collection;
import java.util.UUID;

public interface GrimUser {

    String getName();

    UUID getUniqueId();

    String getBrand();

    int getTransactionPing();

    int getKeepAlivePing();

    String getVersionName();

    double getHorizontalSensitivity();

    double getVerticalSensitivity();

    boolean isVanillaMath();

    void updatePermissions();

    Collection<? extends AbstractCheck> getChecks();

    boolean isLikelySpoofingBrand();

    /**
     * Runs the runnable on the player's netty thread. This may need to be used
     * to access parts of the API safely. This might be removed in future for
     * simplicity.
     * @param runnable
     */
    void runSafely(Runnable runnable);
}
