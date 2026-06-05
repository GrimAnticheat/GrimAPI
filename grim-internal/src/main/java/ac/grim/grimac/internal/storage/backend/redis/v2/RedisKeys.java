package ac.grim.grimac.internal.storage.backend.redis.v2;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Key encoding for Redis v2 adapters. Converts Java key objects into
 * Redis-safe string representations.
 */
@ApiStatus.Internal
final class RedisKeys {

    private RedisKeys() {}

    static @NotNull String encode(@NotNull Object key) {
        if (key instanceof UUID u) return u.toString();
        if (key instanceof Enum<?> e) return String.valueOf(e.ordinal());
        if (key instanceof byte[] b) return java.util.Base64.getEncoder().encodeToString(b);
        return key.toString();
    }
}
