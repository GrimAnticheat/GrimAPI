package ac.grim.grimac.internal.storage.backend.mongo.v2;

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
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Mongo adapter for {@link Counter} stores. Each counter row is one
 * document of shape {@code {_id: <encoded key>, value: <long>}} —
 * dense single-collection layout, atomic per-key increment via
 * {@code $inc}, no read-modify-write.
 *
 * <p>Operation dispatch:
 * <ul>
 *   <li>{@code GetOp} → {@code findOne(_id eq)} project {@code value}; absent doc → 0L</li>
 *   <li>{@code GetManyOp} → {@code find(_id $in)}; absent keys map to 0L</li>
 *   <li>{@code IncrementByOp} → {@code findOneAndUpdate(_id eq, $inc value)} upsert, return AFTER</li>
 *   <li>{@code SetIfHigherOp} → {@code findOneAndUpdate(_id eq, $max value)} upsert, return AFTER</li>
 * </ul>
 *
 * <p>The ring-buffer {@code writeHandler} ingests {@link CounterEvent}
 * deltas — one per submitted event — as a fire-and-forget
 * {@code updateOne($inc)} upsert. Use {@code execute(IncrementByOp)}
 * when the caller needs the post-increment value (e.g. for "did we
 * just cross a threshold" logic).
 *
 * <p>Per-write allocations: one {@code Document} for the filter, one
 * {@code Updates.inc} BSON. {@code $inc}'s server-side atomic add is
 * the property that makes this safe under concurrent writers — no
 * application-level locking required.
 */
@ApiStatus.Internal
public final class MongoCounterAdapter implements KindAdapter<Counter<?>> {

    /** Document field name for the counter's current value. Reserved. */
    public static final String VALUE_FIELD = "value";

    private static final int NAMESPACE_EXISTS = 48;

    private final @NotNull MongoDatabase db;
    private final @NotNull Logger logger;

    private final Map<String, MongoCollection<Document>> collections = new ConcurrentHashMap<>();

    public MongoCounterAdapter(@NotNull MongoDatabase db, @NotNull Logger logger) {
        this.db = db;
        this.logger = logger;
    }

    // ============================== KindAdapter SPI ==============================

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull Class<Counter<?>> kindType() {
        return (Class<Counter<?>>) (Class<?>) Counter.class;
    }

    @Override
    public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_COUNTER, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull Counter<?> kind) throws BackendException {
        String coll = id.name();
        try {
            if (!collectionExists(coll)) {
                try {
                    db.createCollection(coll);
                } catch (com.mongodb.MongoCommandException mce) {
                    if (mce.getErrorCode() != NAMESPACE_EXISTS) throw mce;
                }
            }
            // No secondary indexes: counters are pure key/value, the
            // automatic _id index already gives O(log n) point lookups
            // and any range scan over counter keys would be a design
            // smell (use Entity if you need scans).
        } catch (RuntimeException e) {
            throw new BackendException("failed to ensure counter collection " + coll, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull Counter<?> kind) throws BackendException {
        try {
            db.getCollection(id.name()).drop();
            collections.remove(id.qualified());
        } catch (RuntimeException e) {
            throw new BackendException("failed to drop counter " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id,
            @NotNull Counter<?> kind,
            @NotNull Category<E> category) {
        // Disruptor handler — fire-and-forget atomic $inc per delta.
        // Cast widens K through the wildcard; the runtime check on the
        // event payload type is handled by the Category routing layer.
        @SuppressWarnings({"unchecked", "rawtypes"})
        Counter typed = (Counter) kind;
        return new CounterHandler<>(id, typed, coll(id));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Counter<?> kind, @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof CounterOps.GetOp g)            return (R) Long.valueOf(get(id, g));
            if (op instanceof CounterOps.GetManyOp g)        return (R) getMany(id, g);
            if (op instanceof CounterOps.IncrementByOp i)    return (R) Long.valueOf(incrementBy(id, i));
            if (op instanceof CounterOps.SetIfHigherOp s)    return (R) Long.valueOf(setIfHigher(id, s));
            throw new UnsupportedOperationException(
                "MongoCounterAdapter does not handle " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("mongo counter execute failed for " + op.getClass().getSimpleName() + " on " + id, e);
        }
    }

    @Override
    public @NotNull List<Migration<Counter<?>>> migrations(@NotNull Counter<?> kind) {
        // Counter wire shape is trivial and unchanged across versions
        // so far. Phase 6 might add typed value coercion (e.g. floats)
        // — would land migrations here.
        return List.of();
    }

    // ============================== writeHandler ==============================

    private final class CounterHandler<E> implements StorageEventHandler<E> {
        private final @NotNull StoreId id;
        private final @NotNull MongoCollection<Document> coll;

        CounterHandler(@NotNull StoreId id, @NotNull Counter<?> kind, @NotNull MongoCollection<Document> coll) {
            this.id = id;
            this.coll = coll;
            // kind is currently unused — keyCodec hookup lives in Phase 6
            // when we add non-builtin key types. For Phase 3c the
            // disruptor only carries CounterEvent<K> where K is one of
            // UUID/String/Long/byte[]/Enum (handled by encodeKey).
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                CounterEvent<?> ce = (CounterEvent<?>) event;
                Object key = ce.key;
                if (key == null) {
                    throw new IllegalStateException("CounterEvent.key must be set on " + id);
                }
                long delta = ce.delta;
                if (delta == 0L) return; // no-op write
                // Atomic $inc upsert. $setOnInsert ensures _id is stamped
                // exactly once on first insert; later increments leave
                // _id alone.
                coll.updateOne(
                    Filters.eq("_id", encodeKey(key)),
                    Updates.combine(
                        Updates.inc(VALUE_FIELD, delta),
                        Updates.setOnInsert("_id", encodeKey(key))),
                    new UpdateOptions().upsert(true));
            } catch (RuntimeException e) {
                throw new BackendException("counter increment failed for " + id, e);
            }
        }
    }

    // ============================== execute dispatch ==============================

    private long get(@NotNull StoreId id, @NotNull CounterOps.GetOp<?> op) {
        Document doc = coll(id).find(Filters.eq("_id", encodeKey(op.key())))
            .projection(new Document(VALUE_FIELD, 1).append("_id", 0))
            .first();
        return doc == null ? 0L : extractValue(doc);
    }

    private <K> Map<K, Long> getMany(@NotNull StoreId id, @NotNull CounterOps.GetManyOp<K> op) {
        Collection<K> keys = op.keys();
        if (keys.isEmpty()) return Map.of();
        // Preserve insertion order so callers iterating the result map
        // see results in the order they asked. Default to 0L for any
        // key whose document doesn't exist yet.
        Map<K, Long> out = new LinkedHashMap<>(keys.size() * 4 / 3 + 1);
        for (K k : keys) out.put(k, 0L);
        // Reverse-lookup map from encoded id -> original key so the
        // returned map is keyed by the caller's value, not the
        // BsonBinary / String we wrote to Mongo.
        Map<Object, K> byEncoded = new HashMap<>(keys.size() * 4 / 3 + 1);
        List<Object> encodedIds = new ArrayList<>(keys.size());
        for (K k : keys) {
            Object enc = encodeKey(k);
            encodedIds.add(enc);
            byEncoded.put(enc, k);
        }
        for (Document d : coll(id).find(Filters.in("_id", encodedIds))
                .projection(new Document(VALUE_FIELD, 1))) {
            Object idVal = d.get("_id");
            K originalKey = byEncoded.get(idVal);
            if (originalKey == null) {
                // BsonBinary equality from server may differ from the
                // exact instance we sent — match by encoded byte content
                // when the raw lookup misses.
                originalKey = matchByBytes(byEncoded, idVal);
            }
            if (originalKey != null) {
                out.put(originalKey, extractValue(d));
            }
        }
        return out;
    }

    private long incrementBy(@NotNull StoreId id, @NotNull CounterOps.IncrementByOp<?> op) {
        Document after = coll(id).findOneAndUpdate(
            Filters.eq("_id", encodeKey(op.key())),
            Updates.combine(
                Updates.inc(VALUE_FIELD, op.delta()),
                Updates.setOnInsert("_id", encodeKey(op.key()))),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        return after == null ? op.delta() : extractValue(after);
    }

    private long setIfHigher(@NotNull StoreId id, @NotNull CounterOps.SetIfHigherOp<?> op) {
        // $max promotes the field to the larger of current vs incoming.
        // Atomic, single round-trip. Upsert with $setOnInsert covers
        // first-write where no doc exists.
        Document after = coll(id).findOneAndUpdate(
            Filters.eq("_id", encodeKey(op.key())),
            Updates.combine(
                Updates.max(VALUE_FIELD, op.value()),
                Updates.setOnInsert("_id", encodeKey(op.key()))),
            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        return after == null ? op.value() : extractValue(after);
    }

    // ============================== key + value encoding ==============================

    /**
     * Encode a counter key for use as Mongo {@code _id}. Mirrors
     * {@code MongoEntityAdapter.encodeIdValue} so cross-Kind tooling
     * (admin, migrations) can rely on a single convention.
     */
    static @NotNull Object encodeKey(@NotNull Object key) {
        if (key instanceof UUID u)    return BsonBinaries.uuidBinary(u);
        if (key instanceof Enum<?> e) return e.ordinal();
        if (key instanceof byte[] b)  return new org.bson.BsonBinary(b);
        return key;
    }

    /**
     * Pull the counter value out of a result doc, defaulting to 0L if
     * the field is absent. BSON drivers can return Integer or Long
     * depending on the wire representation — coerce to long here.
     */
    private static long extractValue(@NotNull Document d) {
        Object v = d.get(VALUE_FIELD);
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    /**
     * BsonBinary equality compares by identity-of-bytes through driver
     * versions, but in practice we round-trip via wire so identity
     * doesn't hold. Fall back to matching by raw byte payload for
     * binary-keyed counters (the common case for UUID keys).
     */
    @SuppressWarnings("unchecked")
    private static <K> K matchByBytes(@NotNull Map<Object, K> byEncoded, @NotNull Object idVal) {
        byte[] target = bytesOf(idVal);
        if (target == null) return null;
        for (Map.Entry<Object, K> e : byEncoded.entrySet()) {
            byte[] candidate = bytesOf(e.getKey());
            if (candidate != null && java.util.Arrays.equals(target, candidate)) {
                return e.getValue();
            }
        }
        return null;
    }

    private static byte[] bytesOf(@NotNull Object v) {
        if (v instanceof org.bson.BsonBinary b) return b.getData();
        if (v instanceof org.bson.types.Binary b) return b.getData();
        return null;
    }

    // ============================== misc helpers ==============================

    private boolean collectionExists(@NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    private @NotNull MongoCollection<Document> coll(@NotNull StoreId id) {
        return collections.computeIfAbsent(id.qualified(), k -> db.getCollection(id.name()));
    }
}
