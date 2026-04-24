package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;

import java.util.UUID;

@ApiStatus.Experimental
public final class Deletes {

    private Deletes() {}

    public static ByPlayer byPlayer(UUID uuid) {
        return new ByPlayer(uuid);
    }

    public static OlderThan olderThan(long maxAgeMs) {
        return new OlderThan(maxAgeMs);
    }

    public record ByPlayer(UUID uuid) implements DeleteCriteria {}

    public record OlderThan(long maxAgeMs) implements DeleteCriteria {}
}
