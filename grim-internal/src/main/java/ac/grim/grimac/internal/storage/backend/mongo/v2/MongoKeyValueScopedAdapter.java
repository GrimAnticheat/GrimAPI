package ac.grim.grimac.internal.storage.backend.mongo.v2;

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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Mongo adapter for {@link KeyValueScoped} stores. Replaces the v6
 * "one document per setting" anti-pattern with one document per
 * {@code (scope, scope_key)} tenant, embedding all of that tenant's keys
 * under a {@code values} sub-document.
 *
 * <p>Document shape:
 * <pre>
 *   {
 *     _id: { scope: "PLAYER", scope_key: "&lt;uuid&gt;" },
 *     values: {
 *       "alerts":     &lt;bytes or scalar&gt;,
 *       "custom_cfg": &lt;bytes or scalar&gt;,
 *       ...
 *     },
 *     updated_at: &lt;long epoch ms&gt;
 *   }
 * </pre>
 *
 * <p>{@link KeyValueScopedOps.GetAllOp} = one {@code findOne}; {@link KeyValueScopedOps.PutOp}
 * = one atomic {@code $set: {"values.&lt;key&gt;": value, "updated_at": now}}.
 * Per-tenant load becomes O(1) round-trips regardless of key count.
 *
 * <p>Values are stored opaquely (the codec layer's per-V codec encodes them
 * to {@code byte[]} which Mongo stores as subtype-0 binary). The adapter
 * doesn't introspect value content — that's the extension's responsibility.
 */
@ApiStatus.Internal
public final class MongoKeyValueScopedAdapter implements KindAdapter<KeyValueScoped<?, ?>> {

    private static final String VALUES_FIELD     = "values";
    private static final String UPDATED_FIELD    = "updated_at";
    private static final int NAMESPACE_EXISTS = 48;

    /**
     * Validate KV keys. Mongo's field-name parser treats {@code .} as a path
     * separator inside {@code $set: {"values.<k>": ...}}; if {@code k}
     * literally contained {@code .}, the update would write a nested
     * sub-document instead of a single leaf. {@code $} is reserved as an
     * operator prefix in many contexts. Reject both at the adapter
     * boundary so the bug surfaces at the call site, not in mysterious
     * read-side mismatch.
     */
    private static @NotNull String requireValidKey(@NotNull String key) {
        if (key.isEmpty())          throw new IllegalArgumentException("KV key must be non-empty");
        if (key.indexOf('.') >= 0)  throw new IllegalArgumentException("KV key may not contain '.': " + key);
        if (key.startsWith("$"))    throw new IllegalArgumentException("KV key may not start with '$': " + key);
        return key;
    }

    private final @NotNull MongoDatabase db;
    private final Map<String, MongoCollection<Document>> collections = new ConcurrentHashMap<>();

    /**
     * {@code logger} parameter is currently unused — adapter operations log
     * via thrown {@link BackendException} messages instead. Kept in the
     * signature for symmetry with the other v2 adapters and to leave room
     * for future per-store diagnostic logging without another API change.
     */
    public MongoKeyValueScopedAdapter(@NotNull MongoDatabase db, @NotNull Logger logger) {
        this.db = db;
    }

    // ============================== KindAdapter SPI ==============================

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull Class<KeyValueScoped<?, ?>> kindType() {
        return (Class<KeyValueScoped<?, ?>>) (Class<?>) KeyValueScoped.class;
    }

    @Override
    public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_KV_SCOPED, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) throws BackendException {
        String coll = id.name();
        try {
            if (!collectionExists(coll)) {
                try {
                    db.createCollection(coll);
                } catch (com.mongodb.MongoCommandException mce) {
                    if (mce.getErrorCode() != NAMESPACE_EXISTS) throw mce;
                }
            }
            // No additional index needed: every read/write uses full _id
            // equality, and Mongo's automatic unique _id index already serves
            // that lookup. A separate compound on (_id.scope, _id.scope_key)
            // is dead weight for this adapter's access patterns.
        } catch (RuntimeException e) {
            throw new BackendException("failed to ensure kv-scoped collection " + coll, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) throws BackendException {
        try {
            db.getCollection(id.name()).drop();
            collections.remove(id.qualified());
        } catch (RuntimeException e) {
            throw new BackendException("failed to drop " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id,
            @NotNull KeyValueScoped<?, ?> kind,
            @NotNull Category<E> category) {
        @SuppressWarnings("unchecked")
        StorageEventHandler<E> handler = (StorageEventHandler<E>) new KvHandler(id);
        return handler;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind, @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof KeyValueScopedOps.GetOp<?, ?> g) {
                return (R) get(id, g);
            }
            if (op instanceof KeyValueScopedOps.GetAllOp<?, ?> g) {
                return (R) getAll(id, g);
            }
            if (op instanceof KeyValueScopedOps.PutOp<?, ?> p) {
                put(id, p);
                return null;
            }
            if (op instanceof KeyValueScopedOps.PutAllOp<?, ?> p) {
                putAll(id, p);
                return null;
            }
            if (op instanceof KeyValueScopedOps.RemoveOp<?> r) {
                remove(id, r);
                return null;
            }
            if (op instanceof KeyValueScopedOps.RemoveAllOp<?> r) {
                removeAll(id, r);
                return null;
            }
            if (op instanceof KeyValueScopedOps.CountOp<?> c) {
                return (R) Long.valueOf(count(id, c));
            }
            throw new UnsupportedOperationException(
                "MongoKeyValueScopedAdapter does not handle " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("mongo kv execute failed for " + op.getClass().getSimpleName(), e);
        }
    }

    @Override
    public @NotNull List<Migration<KeyValueScoped<?, ?>>> migrations(@NotNull KeyValueScoped<?, ?> kind) {
        // Per-tenant envelope migration. Idempotent — short-circuits if the
        // canonical collection is already v7-shaped — so safe to register
        // against fresh stores AND extension stores that never had a v6
        // layout.
        return List.of(new MongoSettingsV6ToV7Migration());
    }

    // ============================== ops ==============================

    private java.util.Optional<Object> get(@NotNull StoreId id, @NotNull KeyValueScopedOps.GetOp<?, ?> op) {
        String key = requireValidKey(op.key());
        Document doc = coll(id).find(idFilter(op.scope(), op.scopeKey())).first();
        if (doc == null) return java.util.Optional.empty();
        Document values = doc.get(VALUES_FIELD, Document.class);
        if (values == null) return java.util.Optional.empty();
        Object raw = values.get(key);
        return raw == null ? java.util.Optional.empty() : java.util.Optional.of(unwrapBinary(raw));
    }

    private @NotNull Map<String, Object> getAll(@NotNull StoreId id, @NotNull KeyValueScopedOps.GetAllOp<?, ?> op) {
        Document doc = coll(id).find(idFilter(op.scope(), op.scopeKey())).first();
        if (doc == null) return Map.of();
        Document values = doc.get(VALUES_FIELD, Document.class);
        if (values == null) return Map.of();
        Map<String, Object> out = new LinkedHashMap<>(values.size());
        for (Map.Entry<String, Object> e : values.entrySet()) {
            out.put(e.getKey(), unwrapBinary(e.getValue()));
        }
        return out;
    }

    private void put(@NotNull StoreId id, @NotNull KeyValueScopedOps.PutOp<?, ?> op) {
        String key = requireValidKey(op.key());
        if (op.value() == null) {
            throw new IllegalArgumentException("KV PutOp value must be non-null; use RemoveOp for deletion");
        }
        Document filter = compositeId(op.scope(), op.scopeKey());
        coll(id).updateOne(
            idFilter(op.scope(), op.scopeKey()),
            new Document("$set",
                new Document(VALUES_FIELD + "." + key, wrapValue(op.value()))
                    .append(UPDATED_FIELD, System.currentTimeMillis()))
                .append("$setOnInsert", new Document("_id", filter)),
            new UpdateOptions().upsert(true));
    }

    private void putAll(@NotNull StoreId id, @NotNull KeyValueScopedOps.PutAllOp<?, ?> op) {
        if (op.values().isEmpty()) return;
        Document filter = compositeId(op.scope(), op.scopeKey());
        Document setOps = new Document();
        for (Map.Entry<String, ?> e : op.values().entrySet()) {
            String key = requireValidKey(e.getKey());
            if (e.getValue() == null) {
                throw new IllegalArgumentException("KV PutAllOp value for key '" + key + "' must be non-null");
            }
            setOps.append(VALUES_FIELD + "." + key, wrapValue(e.getValue()));
        }
        setOps.append(UPDATED_FIELD, System.currentTimeMillis());
        coll(id).updateOne(
            idFilter(op.scope(), op.scopeKey()),
            new Document("$set", setOps)
                .append("$setOnInsert", new Document("_id", filter)),
            new UpdateOptions().upsert(true));
    }

    private void remove(@NotNull StoreId id, @NotNull KeyValueScopedOps.RemoveOp<?> op) {
        String key = requireValidKey(op.key());
        coll(id).updateOne(
            idFilter(op.scope(), op.scopeKey()),
            new Document("$unset", new Document(VALUES_FIELD + "." + key, ""))
                .append("$set", new Document(UPDATED_FIELD, System.currentTimeMillis())));
    }

    private void removeAll(@NotNull StoreId id, @NotNull KeyValueScopedOps.RemoveAllOp<?> op) {
        coll(id).deleteOne(idFilter(op.scope(), op.scopeKey()));
    }

    private long count(@NotNull StoreId id, @NotNull KeyValueScopedOps.CountOp<?> op) {
        Document doc = coll(id).find(idFilter(op.scope(), op.scopeKey())).first();
        if (doc == null) return 0L;
        Document values = doc.get(VALUES_FIELD, Document.class);
        return values == null ? 0L : values.size();
    }

    // ============================== handler (ring path) ==============================

    private final class KvHandler implements StorageEventHandler<KeyValueEvent<?, ?>> {
        private final @NotNull StoreId id;

        KvHandler(@NotNull StoreId id) {
            this.id = id;
        }

        @Override
        public void onEvent(KeyValueEvent<?, ?> event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                Object scope = event.scope;
                String scopeKey = event.scopeKey;
                String key = event.key;
                if (scope == null || scopeKey == null || key == null) {
                    throw new IllegalStateException(
                        "KeyValueEvent missing scope/scopeKey/key on " + id);
                }
                requireValidKey(key);
                if (!event.remove && event.value == null) {
                    throw new IllegalStateException(
                        "KeyValueEvent.value is null and event.remove is false on " + id + " (key=" + key + ")");
                }
                if (event.remove) {
                    coll(id).updateOne(
                        idFilter(scope, scopeKey),
                        new Document("$unset", new Document(VALUES_FIELD + "." + key, ""))
                            .append("$set", new Document(UPDATED_FIELD, System.currentTimeMillis())));
                } else {
                    Document filter = compositeId(scope, scopeKey);
                    coll(id).updateOne(
                        idFilter(scope, scopeKey),
                        new Document("$set",
                            new Document(VALUES_FIELD + "." + key, wrapValue(event.value))
                                .append(UPDATED_FIELD, System.currentTimeMillis()))
                            .append("$setOnInsert", new Document("_id", filter)),
                        new UpdateOptions().upsert(true));
                }
            } catch (RuntimeException e) {
                throw new BackendException("kv handler failed for " + id, e);
            } finally {
                event.clear();
            }
        }
    }

    // ============================== helpers ==============================

    private boolean collectionExists(@NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    private @NotNull MongoCollection<Document> coll(@NotNull StoreId id) {
        return collections.computeIfAbsent(id.qualified(), k -> db.getCollection(id.name()));
    }

    private static @NotNull Bson idFilter(@NotNull Object scope, @NotNull String scopeKey) {
        return Filters.eq("_id", compositeId(scope, scopeKey));
    }

    /** Compose the {@code _id} sub-document used as the per-tenant key. */
    private static @NotNull Document compositeId(@NotNull Object scope, @NotNull String scopeKey) {
        return new Document("scope", scopeName(scope)).append("scope_key", scopeKey);
    }

    /** Render a scope value to its on-disk form (enum.name() or its toString()). */
    private static @NotNull String scopeName(@NotNull Object scope) {
        if (scope instanceof Enum<?> e) return e.name();
        return scope.toString();
    }

    /** Box opaque values (e.g. byte[]) into BSON-friendly types. */
    private static @NotNull Object wrapValue(@NotNull Object v) {
        if (v instanceof byte[] b) return new org.bson.BsonBinary(b);
        return v;
    }

    /** Reverse {@link #wrapValue}: unbox stored binary back to byte[]. */
    private static @NotNull Object unwrapBinary(@NotNull Object raw) {
        if (raw instanceof org.bson.types.Binary b) return b.getData();
        if (raw instanceof org.bson.BsonBinary b)  return b.getData();
        return raw;
    }
}
