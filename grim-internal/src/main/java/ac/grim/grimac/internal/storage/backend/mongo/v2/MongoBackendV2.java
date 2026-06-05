package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.kind.Counter;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.internal.storage.backend.mongo.MongoBackendConfig;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Narrow Mongo {@link BackendV2}: owns connection lifecycle + capability
 * advertisement; per-Kind work is delegated to {@link KindAdapter}.
 * <p>
 * Phase 1.2 shipped the {@link MongoEventStreamAdapter}; Phase 2 adds
 * {@link MongoKeyValueScopedAdapter}. Entity / Counter / Blob adapters land
 * in subsequent phases, populating the same registry.
 * <p>
 * Coexists with the legacy {@code MongoBackend} until Phase 3 retires it
 * and renames this to {@code MongoBackend}.
 */
@ApiStatus.Internal
public final class MongoBackendV2 implements BackendV2 {

    private final @NotNull MongoBackendConfig config;
    private MongoClient client;
    private MongoDatabase db;
    private Logger logger;
    private MongoEventStreamAdapter eventStreamAdapter;
    private MongoKeyValueScopedAdapter kvScopedAdapter;
    private MongoEntityAdapter entityAdapter;
    private MongoCounterAdapter counterAdapter;

    public MongoBackendV2(@NotNull MongoBackendConfig config) {
        this.config = config;
    }

    @Override public @NotNull String id() { return "mongo"; }
    @Override public @NotNull ApiVersion apiVersion() { return ApiVersion.CURRENT; }
    @Override public int writerThreads(@NotNull ac.grim.grimac.api.storage.category.Category<?> category) {
        return config.writerThreadsFor(category.id());
    }

    @Override
    public @NotNull EnumSet<Capability> capabilities() {
        // Advertise only what an installed adapter actually supports today.
        // Capability negotiation depends on this being honest — claiming
        // a capability we can't honour breaks registration validation.
        return EnumSet.of(
            Capability.KIND_EVENT_STREAM,
            Capability.KIND_KV_SCOPED,
            Capability.KIND_ENTITY,
            Capability.KIND_COUNTER,
            Capability.BINARY_UUID_KEYS,
            Capability.MULTI_WRITER,
            Capability.ATOMIC_UPSERT,
            // EventStream-specific subcapabilities surfaced at the backend
            // level too so capability negotiation can short-circuit before
            // the adapter is looked up.
            Capability.EVENT_STREAM_TIMESERIES_NATIVE,
            Capability.EVENT_STREAM_TTL_NATIVE,
            Capability.EVENT_STREAM_RANGE_BY_TIME);
    }

    @Override
    public void init(@NotNull BackendContext ctx) throws BackendException {
        this.logger = ctx.logger();
        try {
            Class.forName("com.mongodb.client.MongoClients");
        } catch (ClassNotFoundException cnf) {
            throw new BackendException(
                "mongodb-driver-sync not on the classpath — shade it into the plugin jar or drop it into server/plugins", cnf);
        }
        try {
            this.client = MongoClients.create(config.connectionString());
            this.db = client.getDatabase(config.database());
            this.eventStreamAdapter = new MongoEventStreamAdapter(db, logger, config.batchFlushCap());
            this.kvScopedAdapter = new MongoKeyValueScopedAdapter(db, logger);
            this.entityAdapter = new MongoEntityAdapter(db, logger);
            this.counterAdapter = new MongoCounterAdapter(db, logger);
            // Future: Blob adapter wired here (Phase 6).
        } catch (RuntimeException e) {
            throw new BackendException("failed to initialise Mongo backend v2", e);
        }
    }

    @Override
    public void flush() {
        // Adapters flush per-batch via their handlers. Nothing global.
    }

    @Override
    public void close() throws BackendException {
        try {
            if (client != null) client.close();
        } catch (RuntimeException e) {
            throw new BackendException("failed to close Mongo client", e);
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind) {
        if (kind instanceof EventStream<?, ?>)    return Optional.of((KindAdapter) eventStreamAdapter);
        if (kind instanceof KeyValueScoped<?, ?>) return Optional.of((KindAdapter) kvScopedAdapter);
        if (kind instanceof Entity<?, ?, ?>)      return Optional.of((KindAdapter) entityAdapter);
        if (kind instanceof Counter<?>)           return Optional.of((KindAdapter) counterAdapter);
        // Blob lands in Phase 6.
        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type) {
        if (type.isInstance(db))     return Optional.of((X) db);
        if (type.isInstance(client)) return Optional.of((X) client);
        return Optional.empty();
    }
}
