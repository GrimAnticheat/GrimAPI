package ac.grim.grimac.api.storage.config;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record RetentionRule(boolean enabled, long maxAgeDays) {

    public static final RetentionRule DISABLED = new RetentionRule(false, 0L);

    public long maxAgeMs() {
        return maxAgeDays * 24L * 60L * 60L * 1000L;
    }
}
