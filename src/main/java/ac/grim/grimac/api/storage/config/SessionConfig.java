package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record SessionConfig(long gapMs, boolean scopePerServer) {

    public static SessionConfig defaults() {
        return new SessionConfig(600_000L, true);
    }
}
