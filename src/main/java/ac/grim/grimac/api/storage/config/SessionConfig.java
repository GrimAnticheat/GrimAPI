package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

/**
 * @param gapMs                used by SessionReconstructor at migration time
 *                             to guess session boundaries from V0 violation
 *                             timestamps; live SessionTracker doesn't use it
 *                             (sessions there are bounded by the connection).
 * @param scopePerServer       when true, sessions include the serverName so
 *                             a multi-server proxy can distinguish them.
 * @param heartbeatIntervalMs  how often a connected-player heartbeat is
 *                             emitted to keep {@code last_activity_epoch_ms}
 *                             current. Bounds the inaccuracy on a server
 *                             crash. {@code 0} disables.
 */
@ApiStatus.Experimental
public record SessionConfig(long gapMs, boolean scopePerServer, long heartbeatIntervalMs) {

    public static SessionConfig defaults() {
        return new SessionConfig(600_000L, true, 30_000L);
    }
}
