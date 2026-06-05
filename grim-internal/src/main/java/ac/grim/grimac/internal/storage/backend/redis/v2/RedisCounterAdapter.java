package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.CounterEvent;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.CounterOps;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPool;

import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Redis adapter for {@link Counter} stores. Uses native Redis
 * {@code INCRBY} for atomic increments — no read-modify-write needed.
 * Each counter is a Redis string key: {@code <prefix><storeId>:<encodedKey>}.
 */
@ApiStatus.Internal
public final class RedisCounterAdapter implements KindAdapter<Counter<?>> {

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    private final @NotNull Logger logger;

    public RedisCounterAdapter(@NotNull JedisPool pool, @NotNull String keyPrefix,
                               @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<Counter<?>> kindType() {
        return (Class<Counter<?>>) (Class<?>) Counter.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_COUNTER, Capability.ATOMIC_UPSERT);
    }

    @Override public void ensureStore(@NotNull StoreId id, @NotNull Counter<?> kind) {
        // Redis is schemaless — no DDL needed.
    }

    @Override public void dropStore(@NotNull StoreId id, @NotNull Counter<?> kind) throws BackendException {
        // Would need SCAN + DEL for the prefix. Deferred — dropStore is
        // only called on extension uninstall with data removal.
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull Counter<?> kind, @NotNull Category<E> category) {
        String prefix = keyPrefix + id.name() + ":";
        return (event, sequence, endOfBatch) -> {
            CounterEvent<?> ce = (CounterEvent<?>) event;
            if (ce.key == null || ce.delta == 0L) return;
            try (var j = pool.getResource()) {
                j.incrBy(prefix + RedisKeys.encode(ce.key), ce.delta);
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Counter<?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        String prefix = keyPrefix + id.name() + ":";
        try {
            if (op instanceof CounterOps.GetOp g)
                return (R) Long.valueOf(get(prefix, g));
            if (op instanceof CounterOps.GetManyOp g)
                return (R) getMany(prefix, g);
            if (op instanceof CounterOps.IncrementByOp i)
                return (R) Long.valueOf(incrementBy(prefix, i));
            if (op instanceof CounterOps.SetIfHigherOp s)
                return (R) Long.valueOf(setIfHigher(prefix, s));
            throw new UnsupportedOperationException(
                "RedisCounterAdapter does not handle " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("redis counter failed", e);
        }
    }

    @Override
    public @NotNull List<Migration<Counter<?>>> migrations(@NotNull Counter<?> kind) {
        return List.of();
    }

    private long get(@NotNull String prefix, @NotNull CounterOps.GetOp<?> op) {
        try (var j = pool.getResource()) {
            String val = j.get(prefix + RedisKeys.encode(op.key()));
            return val == null ? 0L : Long.parseLong(val);
        }
    }

    private <K> Map<K, Long> getMany(@NotNull String prefix, @NotNull CounterOps.GetManyOp<K> op) {
        Collection<K> keys = op.keys();
        if (keys.isEmpty()) return Map.of();
        Map<K, Long> out = new LinkedHashMap<>(keys.size() * 4 / 3 + 1);
        String[] redisKeys = new String[keys.size()];
        @SuppressWarnings("unchecked")
        K[] originalKeys = (K[]) keys.toArray();
        int i = 0;
        for (K k : keys) {
            redisKeys[i] = prefix + RedisKeys.encode(k);
            out.put(k, 0L);
            i++;
        }
        try (var j = pool.getResource()) {
            List<String> values = j.mget(redisKeys);
            for (int idx = 0; idx < values.size(); idx++) {
                String v = values.get(idx);
                if (v != null) out.put(originalKeys[idx], Long.parseLong(v));
            }
        }
        return out;
    }

    private long incrementBy(@NotNull String prefix, @NotNull CounterOps.IncrementByOp<?> op) {
        try (var j = pool.getResource()) {
            return j.incrBy(prefix + RedisKeys.encode(op.key()), op.delta());
        }
    }

    private long setIfHigher(@NotNull String prefix, @NotNull CounterOps.SetIfHigherOp<?> op) {
        String key = prefix + RedisKeys.encode(op.key());
        // Redis has no native MAX-upsert. Use a Lua script for atomicity.
        String lua = "local c = tonumber(redis.call('GET', KEYS[1]) or '0') "
            + "if ARGV[1]+0 > c then redis.call('SET', KEYS[1], ARGV[1]) return ARGV[1]+0 "
            + "else return c end";
        try (var j = pool.getResource()) {
            Object result = j.eval(lua, 1, key, String.valueOf(op.value()));
            return result instanceof Number n ? n.longValue() : op.value();
        }
    }
}
