package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@ApiStatus.Experimental
public record PlayerIdentity(
        UUID uuid,
        @Nullable String currentName,
        long firstSeenEpochMs,
        long lastSeenEpochMs) {

    public PlayerIdentity {
        if (uuid == null) throw new IllegalArgumentException("uuid");
    }
}
