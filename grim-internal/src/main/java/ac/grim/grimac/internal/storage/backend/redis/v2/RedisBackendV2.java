package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.*;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.kind.*;
import ac.grim.grimac.internal.storage.backend.redis.RedisBackendConfig;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.EnumSet;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Redis v2 backend. Uses Jedis pool + 4 Redis-specific adapters.
 * Data layout matches the legacy RedisBackend's key patterns so
 * existing Redis data is readable without migration.
 */
@ApiStatus.Internal
public final class RedisBackendV2 implements BackendV2 {

    private final @NotNull RedisBackendConfig config;
    private Logger logger;
    private JedisPool pool;
    private RedisEntityAdapter entityAdapter;
    private RedisEventStreamAdapter eventStreamAdapter;
    private RedisKVScopedAdapter kvScopedAdapter;
    private RedisCounterAdapter counterAdapter;

    public RedisBackendV2(@NotNull RedisBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return "redis-v2"; }
    @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }
    @Override public int writerThreads(@NotNull ac.grim.grimac.api.storage.category.Category<?> category) {
        return config.writerThreadsFor(category.id());
    }
    @Override public @NotNull EnumSet<Capability> capabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.KIND_EVENT_STREAM,
            Capability.KIND_KV_SCOPED, Capability.KIND_COUNTER, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        try {
            Class.forName("redis.clients.jedis.JedisPool");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException("Jedis not on the classpath: " + cnf.getMessage(), cnf);
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        DefaultJedisClientConfig.Builder b = DefaultJedisClientConfig.builder()
            .database(config.database())
            .timeoutMillis(config.timeoutMs());
        if (config.user() != null) b.user(config.user());
        if (config.password() != null) b.password(config.password());
        try {
            this.pool = new JedisPool(poolConfig,
                new HostAndPort(config.host(), config.port()), b.build());
            // Smoke-test connection
            try (var j = pool.getResource()) {
                j.ping();
            }
        } catch (RuntimeException e) {
            throw new BackendException("failed to init Redis pool", e);
        }
        String prefix = config.keyPrefix();
        this.entityAdapter = new RedisEntityAdapter(pool, prefix, logger);
        this.eventStreamAdapter = new RedisEventStreamAdapter(pool, prefix, logger);
        this.kvScopedAdapter = new RedisKVScopedAdapter(pool, prefix, logger);
        this.counterAdapter = new RedisCounterAdapter(pool, prefix, logger);
    }

    @Override public void flush() {}
    @Override public void close() throws BackendException {
        if (pool != null) {
            try { pool.close(); }
            catch (RuntimeException e) { throw new BackendException("Redis close failed", e); }
        }
    }

    @Override @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind) {
        if (kind instanceof Entity<?, ?, ?>)         return Optional.of((KindAdapter) entityAdapter);
        if (kind instanceof EventStream<?, ?>)       return Optional.of((KindAdapter) eventStreamAdapter);
        if (kind instanceof KeyValueScoped<?, ?>)    return Optional.of((KindAdapter) kvScopedAdapter);
        if (kind instanceof Counter<?>)              return Optional.of((KindAdapter) counterAdapter);
        return Optional.empty();
    }
    @Override @SuppressWarnings("unchecked")
    public <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type) {
        if (type.isAssignableFrom(JedisPool.class) && pool != null) return Optional.of((X) pool);
        return Optional.empty();
    }
}
