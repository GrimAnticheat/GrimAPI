package ac.grim.grimac.api;

import ac.grim.grimac.api.checks.AbstractCheckManager;
import ac.grim.grimac.api.common.BasicReloadable;
import ac.grim.grimac.api.config.ConfigReloadable;
import ac.grim.grimac.api.debug.AbstractDebugManager;

import java.util.UUID;

public interface GrimUser extends ConfigReloadable, BasicReloadable {

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

    AbstractCheckManager getCheckManager();

    AbstractDebugManager getDebugManager();

    /**
     * Runs the runnable on the player's netty thread. This may need to be used
     * to access parts of the API safely. This might be removed in future for
     * simplicity.
     * @param runnable
     */
    void runSafely(Runnable runnable);

    boolean isDisabled();

    boolean canModifyPackets();

    void message(String message);

}
