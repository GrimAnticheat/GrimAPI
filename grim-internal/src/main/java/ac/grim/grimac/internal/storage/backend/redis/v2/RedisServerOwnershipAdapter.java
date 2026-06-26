package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipRenewResult;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Redis implementation of the server ownership lease primitive. Mutations use
 * Lua so the read-condition-write decision is atomic.
 */
@ApiStatus.Internal
public final class RedisServerOwnershipAdapter implements ServerOwnershipAdapter {

    private static final String CLAIM_SCRIPT = """
        local closed = redis.call('HGET', KEYS[1], 'closed_at_ms')
        if not closed then
          redis.call('HSET', KEYS[1],
            'persistent_id', ARGV[1],
            'owner_startup_id', ARGV[2],
            'fence', ARGV[3],
            'lease_expires_at_ms', ARGV[5],
            'last_renewed_at_ms', ARGV[4],
            'closed_at_ms', '0',
            'server_name', ARGV[6],
            'hostname', ARGV[7],
            'grim_version', ARGV[8],
            'server_version', ARGV[9])
          redis.call('HDEL', KEYS[1], 'close_reason')
          return 1
        end
        local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_at_ms') or '0')
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        if tonumber(closed) ~= 0 or expires <= tonumber(ARGV[4])
           or (owner == ARGV[2] and fence == ARGV[3]) then
          redis.call('HSET', KEYS[1],
            'persistent_id', ARGV[1],
            'owner_startup_id', ARGV[2],
            'fence', ARGV[3],
            'lease_expires_at_ms', ARGV[5],
            'last_renewed_at_ms', ARGV[4],
            'closed_at_ms', '0',
            'server_name', ARGV[6],
            'hostname', ARGV[7],
            'grim_version', ARGV[8],
            'server_version', ARGV[9])
          redis.call('HDEL', KEYS[1], 'close_reason')
          return 1
        end
        return 0
        """;

    private static final String RENEW_SCRIPT = """
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        local closed = tonumber(redis.call('HGET', KEYS[1], 'closed_at_ms') or '0')
        local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_at_ms') or '0')
        if owner == ARGV[1] and fence == ARGV[2] and closed == 0 and expires > tonumber(ARGV[3]) then
          redis.call('HSET', KEYS[1],
            'lease_expires_at_ms', ARGV[4],
            'last_renewed_at_ms', ARGV[3],
            'closed_at_ms', '0')
          redis.call('HDEL', KEYS[1], 'close_reason')
          return 1
        end
        return 0
        """;

    private static final String CLOSE_SCRIPT = """
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        local closed = tonumber(redis.call('HGET', KEYS[1], 'closed_at_ms') or '0')
        if owner == ARGV[1] and fence == ARGV[2] and closed == 0 then
          redis.call('HSET', KEYS[1],
            'closed_at_ms', ARGV[3],
            'close_reason', ARGV[4])
          return 1
        end
        return 0
        """;

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    @SuppressWarnings("unused")
    private final @NotNull Logger logger;

    public RedisServerOwnershipAdapter(
            @NotNull JedisPool pool,
            @NotNull String keyPrefix,
            @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
    }

    @Override
    public void ensureStore(@NotNull StoreId id) {
        // Redis keys are created lazily by claimOwnership.
    }

    @Override
    public long dbNowEpochMs() throws BackendException {
        try (var j = pool.getResource()) {
            @SuppressWarnings("unchecked")
            List<String> time = (List<String>) j.time();
            long seconds = Long.parseLong(time.get(0));
            long micros = Long.parseLong(time.get(1));
            return seconds * 1000L + micros / 1000L;
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Redis time", e);
        }
    }

    @Override
    public @NotNull OwnershipClaimResult claimOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs,
            @NotNull ServerOwnershipMetadata metadata) throws BackendException {
        try {
            ServerOwnershipSnapshot previous = readOwnership(id, persistentId).orElse(null);
            long now = dbNowEpochMs();
            long expires = now + ttlMs;
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(CLAIM_SCRIPT,
                        List.of(key(id, persistentId)),
                        List.of(
                                persistentId.toString(),
                                startupId.toString(),
                                fence.toString(),
                                Long.toString(now),
                                Long.toString(expires),
                                nullToEmpty(metadata.serverName()),
                                nullToEmpty(metadata.hostname()),
                                nullToEmpty(metadata.grimVersion()),
                                nullToEmpty(metadata.serverVersionString())));
            }
            if (number(result) == 1L) {
                return OwnershipClaimResult.claimed(
                        persistentId, startupId, fence, now, expires, previous);
            }
            ServerOwnershipSnapshot current = readOwnership(id, persistentId).orElse(null);
            if (current == null) {
                return OwnershipClaimResult.claimed(
                        persistentId, startupId, fence, now, expires, previous);
            }
            return OwnershipClaimResult.denied(persistentId, startupId, fence, now, current);
        } catch (RuntimeException e) {
            throw new BackendException("failed to claim Redis server ownership", e);
        }
    }

    @Override
    public @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException {
        try {
            long now = dbNowEpochMs();
            long expires = now + ttlMs;
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(RENEW_SCRIPT,
                        List.of(key(id, persistentId)),
                        List.of(startupId.toString(), fence.toString(),
                                Long.toString(now), Long.toString(expires)));
            }
            if (number(result) == 1L) {
                return OwnershipRenewResult.renewed(persistentId, startupId, fence, now, expires);
            }
            return OwnershipRenewResult.lost(persistentId, startupId, fence, now);
        } catch (RuntimeException e) {
            throw new BackendException("failed to renew Redis server ownership", e);
        }
    }

    @Override
    public boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException {
        try {
            long now = dbNowEpochMs();
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(CLOSE_SCRIPT,
                        List.of(key(id, persistentId)),
                        List.of(startupId.toString(), fence.toString(), Long.toString(now), reason));
            }
            return number(result) == 1L;
        } catch (RuntimeException e) {
            throw new BackendException("failed to close Redis server ownership", e);
        }
    }

    @Override
    public @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException {
        try (var j = pool.getResource()) {
            Map<String, String> h = j.hgetAll(key(id, persistentId));
            if (h == null || h.isEmpty()) return Optional.empty();
            return Optional.of(snapshot(h));
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Redis server ownership", e);
        }
    }

    private @NotNull String key(@NotNull StoreId id, @NotNull UUID persistentId) {
        return keyPrefix + id.name() + ":" + persistentId;
    }

    private static @NotNull ServerOwnershipSnapshot snapshot(@NotNull Map<String, String> h) {
        return new ServerOwnershipSnapshot(
                UUID.fromString(h.get("persistent_id")),
                UUID.fromString(h.get("owner_startup_id")),
                UUID.fromString(h.get("fence")),
                parseLong(h.get("lease_expires_at_ms")),
                parseLong(h.get("last_renewed_at_ms")),
                parseLong(h.get("closed_at_ms")),
                emptyToNull(h.get("close_reason")),
                emptyToNull(h.get("server_name")),
                emptyToNull(h.get("hostname")),
                emptyToNull(h.get("grim_version")),
                emptyToNull(h.get("server_version")));
    }

    private static long parseLong(String raw) {
        return raw == null || raw.isEmpty() ? 0L : Long.parseLong(raw);
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
    }

    private static @NotNull String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
