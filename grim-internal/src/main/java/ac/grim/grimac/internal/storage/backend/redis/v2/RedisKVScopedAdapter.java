package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.KeyValueEvent;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPool;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Redis adapter for {@link KeyValueScoped} stores. One Redis HASH per
 * {@code (scope, scopeKey)} tenant — hash fields are the setting keys,
 * hash values are the raw setting bytes. Key format:
 * {@code <prefix><storeId>:<scope>:<scopeKey>}.
 *
 * <p>Uses the binary Jedis {@code hset/hget/hgetAll/hdel} overloads
 * ({@code byte[]} signatures) so values round-trip arbitrary bytes
 * losslessly — the String overload would corrupt non-UTF-8 payloads.
 * Hash keys and field names are UTF-8.
 */
@ApiStatus.Internal
public final class RedisKVScopedAdapter implements KindAdapter<KeyValueScoped<?, ?>> {

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    private final @NotNull Logger logger;

    public RedisKVScopedAdapter(@NotNull JedisPool pool, @NotNull String keyPrefix,
                                @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
    }

    @SuppressWarnings("unchecked")
    @Override public @NotNull Class<KeyValueScoped<?, ?>> kindType() {
        return (Class<KeyValueScoped<?, ?>>) (Class<?>) KeyValueScoped.class;
    }

    @Override public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_KV_SCOPED, Capability.ATOMIC_UPSERT);
    }

    @Override public void ensureStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) {}
    @Override public void dropStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) {}

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind, @NotNull Category<E> category) {
        String storePrefix = keyPrefix + id.name() + ":";
        return (event, sequence, endOfBatch) -> {
            KeyValueEvent<?, ?> kve = (KeyValueEvent<?, ?>) event;
            if (kve.scope == null || kve.scopeKey == null || kve.key == null) return;
            byte[] hashKeyBytes = bytes(storePrefix + kve.scope + ":" + kve.scopeKey);
            byte[] fieldBytes = bytes(kve.key);
            try (var j = pool.getResource()) {
                if (kve.remove) {
                    j.hdel(hashKeyBytes, fieldBytes);
                } else {
                    if (kve.value == null) return;
                    j.hset(hashKeyBytes, fieldBytes, valueAsBytes(kve.value));
                }
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind,
                         @NotNull Operation<R> op) throws BackendException {
        String storePrefix = keyPrefix + id.name() + ":";
        try {
            if (op instanceof KeyValueScopedOps.GetOp<?, ?> g)       return (R) get(storePrefix, g);
            if (op instanceof KeyValueScopedOps.GetAllOp<?, ?> g)    return (R) getAll(storePrefix, g);
            if (op instanceof KeyValueScopedOps.PutOp<?, ?> p)       { put(storePrefix, p); return null; }
            if (op instanceof KeyValueScopedOps.PutAllOp<?, ?> p)    { putAll(storePrefix, p); return null; }
            if (op instanceof KeyValueScopedOps.RemoveOp<?> r)       { remove(storePrefix, r); return null; }
            if (op instanceof KeyValueScopedOps.RemoveAllOp<?> r)    { removeAll(storePrefix, r); return null; }
            if (op instanceof KeyValueScopedOps.CountOp<?> c)        return (R) Long.valueOf(count(storePrefix, c));
            throw new UnsupportedOperationException("RedisKVScopedAdapter: " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("redis kv failed", e);
        }
    }

    @Override public @NotNull List<Migration<KeyValueScoped<?, ?>>> migrations(@NotNull KeyValueScoped<?, ?> kind) { return List.of(); }

    private byte[] hashKeyBytes(String prefix, Object scope, String scopeKey) {
        return bytes(prefix + scope + ":" + scopeKey);
    }

    private Optional<Object> get(String prefix, KeyValueScopedOps.GetOp<?, ?> op) {
        try (var j = pool.getResource()) {
            byte[] v = j.hget(hashKeyBytes(prefix, op.scope(), op.scopeKey()), bytes(op.key()));
            return v == null ? Optional.empty() : Optional.of(v);
        }
    }

    private Map<String, Object> getAll(String prefix, KeyValueScopedOps.GetAllOp<?, ?> op) {
        try (var j = pool.getResource()) {
            Map<byte[], byte[]> all = j.hgetAll(hashKeyBytes(prefix, op.scope(), op.scopeKey()));
            Map<String, Object> out = new LinkedHashMap<>(all.size());
            all.forEach((k, v) -> {
                if (v != null) out.put(new String(k, java.nio.charset.StandardCharsets.UTF_8), v);
            });
            return out;
        }
    }

    private void put(String prefix, KeyValueScopedOps.PutOp<?, ?> op) throws BackendException {
        try (var j = pool.getResource()) {
            j.hset(hashKeyBytes(prefix, op.scope(), op.scopeKey()),
                bytes(op.key()), valueAsBytes(op.value()));
        }
    }

    private void putAll(String prefix, KeyValueScopedOps.PutAllOp<?, ?> op) throws BackendException {
        Map<byte[], byte[]> fields = new LinkedHashMap<>(op.values().size());
        for (Map.Entry<String, ?> e : op.values().entrySet()) {
            fields.put(bytes(e.getKey()), valueAsBytes(e.getValue()));
        }
        try (var j = pool.getResource()) {
            j.hset(hashKeyBytes(prefix, op.scope(), op.scopeKey()), fields);
        }
    }

    private void remove(String prefix, KeyValueScopedOps.RemoveOp<?> op) {
        try (var j = pool.getResource()) {
            j.hdel(hashKeyBytes(prefix, op.scope(), op.scopeKey()), bytes(op.key()));
        }
    }

    private void removeAll(String prefix, KeyValueScopedOps.RemoveAllOp<?> op) {
        try (var j = pool.getResource()) {
            j.del(hashKeyBytes(prefix, op.scope(), op.scopeKey()));
        }
    }

    private long count(String prefix, KeyValueScopedOps.CountOp<?> op) {
        try (var j = pool.getResource()) {
            return j.hlen(hashKeyBytes(prefix, op.scope(), op.scopeKey()));
        }
    }

    private static byte @NotNull [] bytes(@NotNull String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Coerce a KV value into bytes — same contract as the SQL adapter.
     * byte[] passes through; String is encoded as UTF-8 so callers that
     * still pass legacy String values keep working. Other types fail
     * loud rather than silently round-tripping through {@code toString}.
     */
    private static byte @NotNull [] valueAsBytes(@NotNull Object value) throws BackendException {
        if (value instanceof byte[] b) return b;
        if (value instanceof String s) return bytes(s);
        throw new BackendException("KV value must be byte[] or String, got " + value.getClass().getName());
    }
}
