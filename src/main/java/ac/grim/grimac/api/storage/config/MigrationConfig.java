package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record MigrationConfig(boolean skip, long maxDurationMs) {

    public static MigrationConfig defaults() {
        return new MigrationConfig(false, 0L);
    }
}
