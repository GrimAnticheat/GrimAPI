package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.kind.Granularity;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Cursors;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import ac.grim.grimac.internal.storage.codec.bson.BsonTsCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonTsCodecImpl;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Mongo 5+ adapter for {@link EventStream} stores. Creates a timeseries
 * collection at {@code ensureStore}; writes via the streaming
 * {@link BsonTsCodec} into a pooled {@link MongoBsonBuffer} (one per handler
 * thread); compiles {@code EventStreamOps} records to native Mongo aggregates.
 * <p>
 * Per-write allocation is approximately:
 * <ul>
 *   <li>One {@code Record} (from {@code EventStream.eventToRecord})</li>
 *   <li>One {@code RawBsonDocument} (driver-owned)</li>
 *   <li>{@code N} {@code byte[16]} for UUID partition fields (from {@link BsonBinaries})</li>
 *   <li>{@code N} {@code BsonBinary} wrappers (from {@link BsonBinaries})</li>
 * </ul>
 * The pooled writer buffer is reused; no {@code Document}/HashMap allocations.
 */
@ApiStatus.Internal
public final class MongoEventStreamAdapter implements KindAdapter<EventStream<?, ?>> {

    /** {@code MongoCommandException} error code for "namespace already exists". */
    private static final int NAMESPACE_EXISTS = 48;

    private final @NotNull MongoDatabase db;
    private final @NotNull Logger logger;
    private final int batchFlushCap;

    /** Per-store cached collection handles, keyed by qualified store id. */
    private final Map<String, MongoCollection<Document>> docCollections = new ConcurrentHashMap<>();
    private final Map<String, MongoCollection<RawBsonDocument>> rawCollections = new ConcurrentHashMap<>();

    public MongoEventStreamAdapter(@NotNull MongoDatabase db, @NotNull Logger logger, int batchFlushCap) {
        this.db = db;
        this.logger = logger;
        this.batchFlushCap = batchFlushCap > 0 ? batchFlushCap : 256;
    }

    // ============================== KindAdapter SPI ==============================

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull Class<EventStream<?, ?>> kindType() {
        return (Class<EventStream<?, ?>>) (Class<?>) EventStream.class;
    }

    @Override
    public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(
            Capability.EVENT_STREAM_TIMESERIES_NATIVE,
            Capability.EVENT_STREAM_TTL_NATIVE,
            Capability.EVENT_STREAM_RANGE_BY_TIME,
            Capability.BINARY_UUID_KEYS);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) throws BackendException {
        String coll = id.name();
        try {
            if (!collectionExists(coll)) {
                CreateCollectionOptions opts = new CreateCollectionOptions()
                    .timeSeriesOptions(buildTimeSeriesOptions(kind));
                if (kind.retention() != null) {
                    opts.expireAfter(kind.retention().toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
                }
                try {
                    db.createCollection(coll, opts);
                    logger.info(() -> "created timeseries collection " + coll
                        + " (meta=" + kind.partitionFields()
                        + ", time=" + kind.timestampField() + ")");
                } catch (com.mongodb.MongoCommandException mce) {
                    // Another instance won the race (MULTI_WRITER).
                    if (mce.getErrorCode() != NAMESPACE_EXISTS) throw mce;
                    logger.fine(() -> "timeseries collection " + coll
                        + " was created concurrently by another instance; continuing");
                }
            }
            ensureIndexes(coll, kind);
        } catch (RuntimeException e) {
            throw new BackendException("failed to ensure timeseries collection " + coll, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull EventStream<?, ?> kind) throws BackendException {
        try {
            db.getCollection(id.name()).drop();
            docCollections.remove(id.qualified());
            rawCollections.remove(id.qualified());
        } catch (RuntimeException e) {
            throw new BackendException("failed to drop " + id, e);
        }
    }

    @Override
    public <E> @NotNull StorageEventHandler<E> writeHandler(
            @NotNull StoreId id,
            @NotNull EventStream<?, ?> kind,
            @NotNull Category<E> category) {
        @SuppressWarnings("unchecked")
        EventStream<E, ?> typed = (EventStream<E, ?>) kind;
        return new EventStreamHandler<>(id, typed, rawCollection(id));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof EventStreamOps.PageOp<?> p) {
                return (R) page(id, kind, p);
            }
            if (op instanceof EventStreamOps.RangeByTimeOp<?> r) {
                return (R) rangeByTime(id, kind, r);
            }
            if (op instanceof EventStreamOps.CountOp c) {
                return (R) Long.valueOf(count(id, c));
            }
            if (op instanceof EventStreamOps.CountManyOp<?> c) {
                return (R) countMany(id, c);
            }
            if (op instanceof EventStreamOps.CountDistinctOp c) {
                return (R) Long.valueOf(countDistinct(id, c));
            }
            if (op instanceof EventStreamOps.DeleteByPartitionOp d) {
                deleteByPartition(id, d);
                return null;
            }
            if (op instanceof EventStreamOps.DeleteOlderThanOp d) {
                deleteOlderThan(id, kind, d);
                return null;
            }
            throw new UnsupportedOperationException(
                "MongoEventStreamAdapter does not handle " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("mongo execute failed for " + op.getClass().getSimpleName() + " on " + id, e);
        }
    }

    @Override
    public @NotNull List<Migration<EventStream<?, ?>>> migrations(@NotNull EventStream<?, ?> kind) {
        // Single migration recipe: v6 (flat regular collection) → v7
        // (timeseries with meta sub-doc). Applies to every registered
        // EventStream on this backend; the migration itself short-circuits
        // when the canonical collection is already timeseries-shaped, so
        // it's safe to register against fresh stores too.
        return List.of(new MongoViolationsV6ToV7Migration());
    }

    // ============================== ensureStore helpers ==============================

    private boolean collectionExists(@NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    private @NotNull TimeSeriesOptions buildTimeSeriesOptions(@NotNull EventStream<?, ?> kind) {
        TimeSeriesOptions opts = new TimeSeriesOptions(kind.timestampField()).metaField("meta");
        opts.granularity(toMongoGranularity(kind.granularity()));
        return opts;
    }

    private @NotNull TimeSeriesGranularity toMongoGranularity(@NotNull Granularity g) {
        return switch (g) {
            case SECONDS -> TimeSeriesGranularity.SECONDS;
            case MINUTES -> TimeSeriesGranularity.MINUTES;
            case HOURS   -> TimeSeriesGranularity.HOURS;
        };
    }

    /**
     * Indexes on a timeseries collection are restricted: only on metaField
     * subfields, timeField, or fields that are themselves index-able non-data
     * fields (like {@code id}). We create:
     * <ul>
     *   <li>a timeField index so {@code rangeByTime} + sorted page reads
     *       don't degrade on Mongo 5.x (6.3+ auto-creates a metaField+timeField
     *       compound but we can't rely on that);</li>
     *   <li>a per-partition {@code meta.<field>} index so {@code countMany}'s
     *       {@code $in} match is cheap;</li>
     *   <li>a compound {@code (meta.<partition>, timeField)} index per
     *       partition so paged reads with a partition filter exploit
     *       index ordering.</li>
     * </ul>
     * {@code createIndex} is idempotent — Mongo returns the existing index
     * name when the spec matches.
     */
    private void ensureIndexes(@NotNull String coll, @NotNull EventStream<?, ?> kind) {
        MongoCollection<Document> c = db.getCollection(coll);
        c.createIndex(Indexes.ascending(kind.timestampField()));
        for (String partition : kind.partitionFields()) {
            c.createIndex(Indexes.ascending("meta." + partition));
            c.createIndex(Indexes.compoundIndex(
                Indexes.ascending("meta." + partition),
                Indexes.ascending(kind.timestampField())));
        }
    }

    // ============================== writeHandler ==============================

    /** Per-thread BSON buffer pool. One {@link MongoBsonBuffer} per JVM thread. */
    private static final ThreadLocal<MongoBsonBuffer> BSON_BUFFER = ThreadLocal.withInitial(MongoBsonBuffer::new);

    private final class EventStreamHandler<E> implements StorageEventHandler<E> {

        private final @NotNull StoreId id;
        private final @NotNull MongoCollection<RawBsonDocument> coll;
        private final @NotNull Function<E, Object> eventToRecord;
        @SuppressWarnings({"rawtypes", "unchecked"})
        private final @NotNull BsonTsCodec codec;
        private final @NotNull List<RawBsonDocument> pending = new ArrayList<>();
        private final int flushCap;

        @SuppressWarnings({"rawtypes", "unchecked"})
        EventStreamHandler(@NotNull StoreId id, @NotNull EventStream<E, ?> kind, @NotNull MongoCollection<RawBsonDocument> coll) {
            this.id = id;
            this.coll = coll;
            this.eventToRecord = (Function) kind.eventToRecord();
            this.codec = BsonCodecs.timeseries(kind.recordType());
            this.flushCap = batchFlushCap;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                Object record = eventToRecord.apply(event);
                MongoBsonBuffer pool = BSON_BUFFER.get();
                org.bson.BsonBinaryWriter writer = pool.writer();
                try {
                    codec.encode(record, writer);
                } finally {
                    writer.close();
                }
                pending.add(pool.snapshot());
                if (endOfBatch || pending.size() >= flushCap) {
                    flush();
                }
            } catch (RuntimeException e) {
                throw new BackendException("encode failed for " + id, e);
            }
        }

        private void flush() throws BackendException {
            if (pending.isEmpty()) return;
            try {
                coll.insertMany(pending);
            } catch (RuntimeException e) {
                throw new BackendException("insert failed for " + id, e);
            } finally {
                pending.clear();
            }
        }
    }

    // ============================== execute dispatch ==============================

    private <R> Page<R> page(@NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull EventStreamOps.PageOp<?> p) {
        Bson partitionFilter = partitionEq(p.partition(), p.key());
        Bson filter = applyCursor(partitionFilter, kind, p.cursor(), p.partition());
        int pageSize = Math.max(1, p.pageSize());
        List<Document> docs = new ArrayList<>(pageSize + 1);
        rawDocColl(id).find(filter)
            .sort(new Document(kind.timestampField(), 1).append("id", 1))
            .limit(pageSize + 1)
            .into(docs);
        return materialise(kind, docs, pageSize);
    }

    private <R> Page<R> rangeByTime(@NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull EventStreamOps.RangeByTimeOp<?> r) {
        Bson timeFilter = Filters.and(
            Filters.gte(kind.timestampField(), new Date(r.fromEpochMs())),
            Filters.lt(kind.timestampField(), new Date(r.toEpochMs())));
        Bson filter = applyCursor(timeFilter, kind, r.cursor(), null);
        int pageSize = Math.max(1, r.pageSize());
        List<Document> docs = new ArrayList<>(pageSize + 1);
        rawDocColl(id).find(filter)
            .sort(new Document(kind.timestampField(), 1).append("id", 1))
            .limit(pageSize + 1)
            .into(docs);
        return materialise(kind, docs, pageSize);
    }

    private long count(@NotNull StoreId id, @NotNull EventStreamOps.CountOp op) {
        return rawDocColl(id).countDocuments(partitionEq(op.partition(), op.key()));
    }

    private <K> Map<K, Long> countMany(@NotNull StoreId id, @NotNull EventStreamOps.CountManyOp<K> op) {
        Collection<K> keys = op.keys();
        if (keys.isEmpty()) return Collections.emptyMap();
        List<Object> encodedKeys = new ArrayList<>(keys.size());
        for (K k : keys) encodedKeys.add(encodePartitionValue(k));
        List<Bson> pipeline = List.of(
            Aggregates.match(Filters.in("meta." + op.partition(), encodedKeys)),
            Aggregates.group("$meta." + op.partition(), List.of(
                com.mongodb.client.model.Accumulators.sum("n", 1))));
        Map<K, Long> out = new HashMap<>(keys.size() * 2);
        for (K k : keys) out.put(k, 0L);
        for (Document d : rawDocColl(id).aggregate(pipeline)) {
            Object raw = d.get("_id");
            Object decoded = decodePartitionValue(raw, sampleKey(keys));
            @SuppressWarnings("unchecked")
            K k = (K) decoded;
            long n = ((Number) d.get("n")).longValue();
            out.put(k, n);
        }
        return out;
    }

    private long countDistinct(@NotNull StoreId id, @NotNull EventStreamOps.CountDistinctOp op) {
        List<Bson> pipeline = List.of(
            Aggregates.match(partitionEq(op.partition(), op.key())),
            Aggregates.group("$meta." + op.field()),
            Aggregates.count("n"));
        Document first = rawDocColl(id).aggregate(pipeline).first();
        return first == null ? 0L : first.getInteger("n", 0);
    }

    private void deleteByPartition(@NotNull StoreId id, @NotNull EventStreamOps.DeleteByPartitionOp op) {
        rawDocColl(id).deleteMany(partitionEq(op.partition(), op.key()));
    }

    /**
     * NOTE: this op requires Mongo 5.0.6+ on a timeseries collection. Mongo
     * 5.0.5 only permits time-series deletes whose query matches metaField;
     * a pure timestamp filter fails on that patch. For retention-based
     * cleanup, prefer the native TTL set via {@code kind.retention()} at
     * {@code ensureStore} (works on every 5.0.x).
     */
    private void deleteOlderThan(@NotNull StoreId id, @NotNull EventStream<?, ?> kind, @NotNull EventStreamOps.DeleteOlderThanOp op) {
        rawDocColl(id).deleteMany(Filters.lt(kind.timestampField(), new Date(op.cutoffEpochMs())));
    }

    // ============================== read materialisation ==============================

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <R> Page<R> materialise(@NotNull EventStream<?, ?> kind, @NotNull List<Document> docs, int pageSize) {
        BsonTsCodec codec = BsonCodecs.timeseries(kind.recordType());
        boolean hasMore = docs.size() > pageSize;
        List<R> items = new ArrayList<>(Math.min(docs.size(), pageSize));
        Document last = null;
        int n = Math.min(docs.size(), pageSize);
        for (int i = 0; i < n; i++) {
            Document d = docs.get(i);
            items.add((R) codec.decode(d));
            last = d;
        }
        Cursor cursor = (hasMore && last != null) ? encodeCursor(last, kind) : null;
        return new Page<>(items, cursor);
    }

    /**
     * Encode the next-page cursor using the portable canonical format
     * (see {@link Cursors}). For EventStream the pair is
     * {@code (occurred_at epoch ms, 16-byte UUIDv7 id)}.
     */
    private @NotNull Cursor encodeCursor(@NotNull Document last, @NotNull EventStream<?, ?> kind) {
        Object ts = last.get(kind.timestampField());
        long tsMs = ts instanceof Date d ? d.getTime() : ((Number) ts).longValue();
        Object idRaw = last.get("id");
        byte[] idBytes;
        if (idRaw instanceof org.bson.types.Binary b) idBytes = b.getData();
        else if (idRaw instanceof org.bson.BsonBinary b) idBytes = b.getData();
        else if (idRaw instanceof byte[] b) idBytes = b;
        else throw new IllegalStateException(
            "unexpected id encoding: " + (idRaw == null ? "null" : idRaw.getClass().getName()));
        return Cursors.encode(tsMs, idBytes);
    }

    private @NotNull Bson applyCursor(
            @NotNull Bson base,
            @NotNull EventStream<?, ?> kind,
            @Nullable Cursor cursor,
            @Nullable String partitionField) {
        if (cursor == null) return base;
        Cursors.Decoded d = Cursors.decode(cursor);
        // CRITICAL: wrap idBytes in a BsonBinary with UUID_STANDARD subtype.
        // The Java driver encodes raw byte[] as BinData subtype 0; writes use
        // subtype 4. Mongo's BinData comparison goes (length, subtype, bytes),
        // so a raw-bytes operand at the same timestamp compares LESS than every
        // stored UUID — the seek skips nothing and the same page repeats.
        org.bson.BsonBinary idBinary = new org.bson.BsonBinary(
            org.bson.BsonBinarySubType.UUID_STANDARD, d.idBytes());
        Bson seek = Filters.or(
            Filters.gt(kind.timestampField(), new Date(d.orderedKey())),
            Filters.and(
                Filters.eq(kind.timestampField(), new Date(d.orderedKey())),
                Filters.gt("id", idBinary)));
        return Filters.and(base, seek);
    }

    // ============================== partition encoding ==============================

    /** Build a `meta.<field>` equality filter for a partition key. */
    private @NotNull Bson partitionEq(@NotNull String field, @NotNull Object key) {
        return Filters.eq("meta." + field, encodePartitionValue(key));
    }

    /**
     * Encode a Java-side partition value into the BSON shape the codec
     * writes into the meta sub-document. Must mirror the codec's encoding
     * exactly — otherwise filters won't match stored values.
     * <ul>
     *   <li>UUID → 16-byte BsonBinary subtype 4 (mirrors BsonTsCodecImpl)</li>
     *   <li>Enum → int32 ordinal (mirrors BsonTsCodecImpl's readEnumOrdinal)</li>
     *   <li>byte[] → wrapped as BsonBinary subtype 0</li>
     *   <li>everything else → driver default coercion (int, long, double, String, Boolean)</li>
     * </ul>
     */
    private @NotNull Object encodePartitionValue(@NotNull Object key) {
        if (key instanceof java.util.UUID u) return BsonBinaries.uuidBinary(u);
        if (key instanceof Enum<?> e)        return e.ordinal();
        if (key instanceof byte[] b)         return new org.bson.BsonBinary(b);
        return key;
    }

    private @NotNull Object decodePartitionValue(@NotNull Object raw, @NotNull Object sample) {
        if (sample instanceof java.util.UUID) return BsonBinaries.toUuid(raw);
        if (sample instanceof Enum<?> e) {
            int ordinal = raw instanceof Number n ? n.intValue() : Integer.parseInt(raw.toString());
            return e.getClass().getEnumConstants()[ordinal];
        }
        return raw;
    }

    private static <K> @NotNull K sampleKey(@NotNull Collection<K> keys) {
        for (K k : keys) return k;
        throw new IllegalStateException("empty keys");
    }

    // ============================== collection handles ==============================

    private @NotNull MongoCollection<RawBsonDocument> rawCollection(@NotNull StoreId id) {
        return rawCollections.computeIfAbsent(id.qualified(),
            k -> db.getCollection(id.name(), RawBsonDocument.class));
    }

    private @NotNull MongoCollection<Document> rawDocColl(@NotNull StoreId id) {
        return docCollections.computeIfAbsent(id.qualified(),
            k -> db.getCollection(id.name()));
    }
}
