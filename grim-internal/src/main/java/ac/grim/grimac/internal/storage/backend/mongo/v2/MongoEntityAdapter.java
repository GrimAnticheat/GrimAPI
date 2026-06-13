package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.query.Cursor;
import ac.grim.grimac.api.storage.query.Cursors;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodec;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecImpl;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Mongo adapter for {@link Entity} stores. Each record is one document
 * keyed by the {@link Entity}'s declared {@code @Id} field, stored at
 * the top level under {@code _id}. Writes are atomic upserts via
 * {@code $set} + {@code $setOnInsert} — no read-modify-write cycle.
 *
 * <p>Secondary indexes from the Kind's {@code secondaryIndexes()} are
 * created at {@link #ensureStore}; supports ascending/descending via the
 * {@code "-fieldname"} prefix convention.
 *
 * <p>Operation dispatch:
 * <ul>
 *   <li>{@code GetByIdOp} → {@code findOne(_id eq)}</li>
 *   <li>{@code GetManyOp} → {@code find(_id $in)}</li>
 *   <li>{@code FindByIndexOp} → {@code find(field eq) + sort + limit}</li>
 *   <li>{@code PrefixIndexOp} → {@code find(field $regex ^prefix)}</li>
 *   <li>{@code CountByIndexOp} → {@code countDocuments(field eq)}</li>
 *   <li>{@code DeleteByIdOp} → {@code deleteOne(_id eq)}</li>
 * </ul>
 *
 * <p>The write handler converts {@code Entity.eventToRecord} output → BSON
 * via {@link BsonCodec} into a pooled buffer, then issues a {@code $set}
 * upsert. Per-write allocations match the EventStream adapter: one
 * record, one RawBsonDocument, plus codec-level UUID byte[]/BsonBinary.
 *
 * <p><strong>Merge semantics.</strong> Each upsert dispatches to one
 * of two paths based on the kind's {@link EncodeShape} field merge
 * modes: a fast path that streams a {@link RawBsonDocument}
 * {@code $set} when every field is {@link MergeMode#OVERWRITE}, and
 * an aggregation-pipeline path when any field carries
 * {@code @InsertOnly} / {@code @PreserveOnNonNull} / {@code @MergeMax}
 * / {@code @MergeMin}. The pipeline compiles those modes to
 * {@code $cond}/{@code $ifNull}/{@code $max}/{@code $min} expressions
 * over the existing-vs-incoming pair so concurrent heartbeats from
 * partially-wired producers don't clobber values they didn't intend
 * to overwrite (close timestamps, owning instance ids, monotonic
 * timestamps, surrogate ids). See {@link MergeMode} for the per-mode
 * compilation table.
 *
 * <p>{@link ac.grim.grimac.api.storage.codec.Sentinel @Sentinel}
 * extends preserve semantics to primitive-long fields with an
 * "unset" sentinel value (e.g. {@code SessionRecord.closedAtEpochMs}
 * with {@code OPEN = 0L}): the merge stage emits a {@code $cond} that
 * treats {@code $col == sentinel} the same as null/missing. Lets a
 * primitive-long heartbeat carry the sentinel without clobbering a
 * real prior write.
 *
 * <p><strong>Mixed-writer caveat.</strong> The {@code @Sentinel}
 * pipeline canonicalises absent / explicit-null / sentinel-valued
 * existing rows by writing the sentinel-typed value (e.g. {@code 0L}
 * instead of {@code null}) on the first v2 upsert. If the legacy
 * {@code MongoBackend} is still writing concurrently to the same
 * collection, its {@code $ifNull} preserve logic on {@code closed_at}
 * would see the now-non-null {@code 0L} as "already set" and refuse
 * to write a real close timestamp. Cutover must be clean — stop the
 * legacy writers before starting v2 writers. Same hazard applies to
 * SQL via the {@code COALESCE} path. Documented but not enforced
 * here; the {@code DataStoreLifecycle} wiring (Phase 1.4c) is
 * responsible for choosing one path at a time.
 *
 * <p>Phase 3a shipped the adapter shape; concrete Entity Kind
 * declarations for sessions / players / checks landed in Phase 3b;
 * per-field merge semantics including the primitive-long sentinel
 * pattern shipped in Phase 5b.
 */
@ApiStatus.Internal
public final class MongoEntityAdapter implements KindAdapter<Entity<?, ?, ?>> {

    private static final int NAMESPACE_EXISTS = 48;
    /**
     * Mongo error code 85 — {@code IndexOptionsConflict}. Raised when
     * {@code createIndex} is called with a key spec that matches an
     * existing index, but the requested options (typically just the
     * name) differ. We treat this as success — the index is already
     * there with equivalent semantics, just under a different name —
     * so v2 {@code ensureStore} doesn't fail against a collection
     * previously initialised by the legacy backend (e.g. legacy created
     * {@code (player_uuid, started_at DESC)} as
     * {@code idx_grim_sessions_player_started}; v2 wants
     * {@code by_player_started}).
     */
    private static final int INDEX_OPTIONS_CONFLICT = 85;

    private final @NotNull MongoDatabase db;
    private final @NotNull Logger logger;

    private final Map<String, MongoCollection<Document>> docCollections = new ConcurrentHashMap<>();
    private final Map<String, MongoCollection<RawBsonDocument>> rawCollections = new ConcurrentHashMap<>();

    public MongoEntityAdapter(@NotNull MongoDatabase db, @NotNull Logger logger) {
        this.db = db;
        this.logger = logger;
    }

    // ============================== KindAdapter SPI ==============================

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull Class<Entity<?, ?, ?>> kindType() {
        return (Class<Entity<?, ?, ?>>) (Class<?>) Entity.class;
    }

    @Override
    public @NotNull EnumSet<Capability> subcapabilities() {
        return EnumSet.of(Capability.KIND_ENTITY, Capability.ATOMIC_UPSERT);
    }

    @Override
    public void ensureStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws BackendException {
        String coll = id.name();
        try {
            if (!collectionExists(coll)) {
                try {
                    db.createCollection(coll);
                } catch (com.mongodb.MongoCommandException mce) {
                    if (mce.getErrorCode() != NAMESPACE_EXISTS) throw mce;
                }
            }
            MongoCollection<Document> c = db.getCollection(coll);
            for (IndexSpec spec : kind.secondaryIndexes()) {
                createIndexIgnoringConflict(c, buildIndex(spec),
                    new IndexOptions().unique(spec.unique()).name(spec.name()));
                if (spec.caseInsensitivePrefix()) {
                    // Companion lowercased-prefix index. Application is responsible
                    // for writing both the original + _lower variant; the codec
                    // doesn't auto-generate _lower fields. Convention only.
                    for (String f : spec.fields()) {
                        String field = stripDir(f);
                        createIndexIgnoringConflict(c, Indexes.ascending(field + "_lower"),
                            new IndexOptions().name(spec.name() + "_lower"));
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new BackendException("failed to ensure entity collection " + coll, e);
        }
    }

    @Override
    public void dropStore(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws BackendException {
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
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Category<E> category) {
        @SuppressWarnings("unchecked")
        Entity<?, E, ?> typed = (Entity<?, E, ?>) kind;
        return new EntityHandler<>(id, typed, rawCollection(id));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <R> R execute(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull Operation<R> op) throws BackendException {
        try {
            if (op instanceof EntityOps.UpsertOp u)       { upsertRecord(id, kind, u.record()); return null; }
            if (op instanceof EntityOps.GetByIdOp g)         return (R) getById(id, kind, g);
            if (op instanceof EntityOps.GetManyOp g)         return (R) getMany(id, kind, g);
            if (op instanceof EntityOps.FindByIndexOp f)     return (R) findByIndex(id, kind, f);
            if (op instanceof EntityOps.PrefixIndexOp p)     return (R) prefixIndex(id, kind, p);
            if (op instanceof EntityOps.CountByIndexOp c)    return (R) Long.valueOf(countByIndex(id, kind, c));
            if (op instanceof EntityOps.DeleteByIdOp d)    { deleteById(id, d); return null; }
            if (op instanceof EntityOps.DeleteByIndexOp d) { deleteByIndex(id, kind, d); return null; }
            throw new UnsupportedOperationException(
                "MongoEntityAdapter does not handle " + op.getClass().getName());
        } catch (RuntimeException e) {
            throw new BackendException("mongo entity execute failed for " + op.getClass().getSimpleName() + " on " + id, e);
        }
    }

    @Override
    public @NotNull List<Migration<Entity<?, ?, ?>>> migrations(@NotNull Entity<?, ?, ?> kind) {
        // Every UUID-keyed Entity needs the subtype rewrite. The
        // migration itself short-circuits if the kind has no UUID
        // fields, so it's safe to attach unconditionally.
        return List.of(new MongoEntityV6ToV7UuidSubtypeMigration());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void upsertRecord(
            @NotNull StoreId id,
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Object record) throws BackendException {
        if (!kind.recordType().isInstance(record)) {
            throw new IllegalArgumentException("record type " + record.getClass().getName()
                    + " does not match entity " + kind.recordType().getName());
        }
        new EntityHandler(id, kind, rawCollection(id)).upsertRecord(record);
    }

    // ============================== ensureStore helpers ==============================

    private boolean collectionExists(@NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    /**
     * Wrapper for {@code createIndex} that swallows
     * {@link #INDEX_OPTIONS_CONFLICT} (Mongo error 85). Lets v2
     * {@code ensureStore} run idempotently against a collection
     * whose indexes were already created by the legacy MongoBackend
     * with different names. Any other Mongo error is rethrown
     * unchanged.
     */
    private void createIndexIgnoringConflict(@NotNull MongoCollection<Document> c,
                                              @NotNull Bson keys,
                                              @NotNull IndexOptions opts) {
        try {
            c.createIndex(keys, opts);
        } catch (com.mongodb.MongoCommandException mce) {
            if (mce.getErrorCode() == INDEX_OPTIONS_CONFLICT) {
                logger.fine(() -> "index " + opts.getName()
                    + " conflicts with existing equivalent key on " + c.getNamespace().getCollectionName()
                    + " — treating as success");
                return;
            }
            throw mce;
        }
    }

    private static @NotNull Bson buildIndex(@NotNull IndexSpec spec) {
        if (spec.fields().size() == 1) {
            String f = spec.fields().get(0);
            return f.startsWith("-") ? Indexes.descending(stripDir(f)) : Indexes.ascending(stripDir(f));
        }
        List<Bson> parts = new ArrayList<>(spec.fields().size());
        for (String f : spec.fields()) {
            parts.add(f.startsWith("-") ? Indexes.descending(stripDir(f)) : Indexes.ascending(stripDir(f)));
        }
        return Indexes.compoundIndex(parts);
    }

    private static @NotNull String stripDir(@NotNull String f) {
        return f.startsWith("-") ? f.substring(1) : f;
    }

    // ============================== writeHandler ==============================

    /** Per-thread BSON buffer pool, shared with EventStream adapter. */
    private static final ThreadLocal<MongoBsonBuffer> BSON_BUFFER =
        ThreadLocal.withInitial(MongoBsonBuffer::new);

    private final class EntityHandler<E> implements StorageEventHandler<E> {

        private final @NotNull StoreId id;
        private final @NotNull MongoCollection<RawBsonDocument> coll;
        private final @NotNull MongoCollection<Document> docColl;
        private final @NotNull Function<E, Object> eventToRecord;
        @SuppressWarnings("rawtypes")
        private final @NotNull BsonCodec codec;
        private final @NotNull EncodeShape shape;
        private final @NotNull Function<Object, Object> idExtractor;
        private final @NotNull String idField;
        /**
         * Resolved companion descriptors: each entry says "for the field
         * with persistent index N, also write a String at <columnName>"
         * containing the lowercased value. Populated at construction time
         * from any {@link IndexSpec#caseInsensitivePrefix()}=true index;
         * empty when the kind declares no such indexes.
         */
        private final @NotNull LowerCompanion[] lowerCompanions;
        private final @NotNull UpsertPlan upsertPlan;

        @SuppressWarnings({"rawtypes", "unchecked"})
        EntityHandler(@NotNull StoreId id, @NotNull Entity<?, E, ?> kind, @NotNull MongoCollection<RawBsonDocument> coll) {
            this.id = id;
            this.coll = coll;
            this.docColl = docColl(id);
            this.eventToRecord = (Function) kind.eventToRecord();
            this.codec = BsonCodecs.regular(kind.recordType());
            this.shape = kind.codec().shape();
            this.idExtractor = (Function) kind.idOf();
            this.idField = shape.idField();
            this.lowerCompanions = buildLowerCompanions(kind);
            this.upsertPlan = UpsertPlan.of(shape);
        }

        @Override
        public void onEvent(E event, long sequence, boolean endOfBatch) throws BackendException {
            try {
                Object record = eventToRecord.apply(event);
                upsertRecord(record);
            } catch (RuntimeException e) {
                throw new BackendException("entity upsert failed for " + id, e);
            }
        }

        private void upsertRecord(@NotNull Object record) {
            Object idValue = idExtractor.apply(record);
            upsertPlan.write(this, record, idValue);
        }

        /**
         * Fast path: no per-field merge semantics needed, so the whole
         * encoded record can be stamped via a single {@code $set} of a
         * RawBsonDocument. Allocation cost: one record, one
         * RawBsonDocument, plus codec-level UUID byte[]/BsonBinary.
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private void writeRawBson(@NotNull Object record, @NotNull Object idValue) {
            MongoBsonBuffer pool = BSON_BUFFER.get();
            BsonBinaryWriter writer = pool.writer();
            try {
                if (lowerCompanions.length == 0) {
                    codec.encode(record, writer);
                } else {
                    writer.writeStartDocument();
                    codec.encodeFields(record, writer);
                    for (LowerCompanion lc : lowerCompanions) {
                        Object raw = codec.readField(record, lc.fieldIndex);
                        if (raw instanceof String s) {
                            writer.writeString(lc.columnName, s.toLowerCase(java.util.Locale.ROOT));
                        }
                        // Skip on null/non-String source: matches the
                        // merge path and the codec's nullable-field
                        // semantics (absent rather than explicit null).
                        // Either form satisfies prefix queries (regex
                        // never matches null/missing); skipping keeps
                        // the wire format consistent across paths.
                    }
                    writer.writeEndDocument();
                }
            } finally {
                writer.close();
            }
            RawBsonDocument encoded = pool.snapshot();
            coll.updateOne(
                Filters.eq("_id", encodeIdValue(idValue)),
                new Document("$set", encoded)
                    .append("$setOnInsert", new Document("_id", encodeIdValue(idValue))),
                new UpdateOptions().upsert(true));
        }

        /**
         * Merge-aware path: an aggregation-pipeline update that splits
         * the record's fields into two {@code $set} stages — first the
         * OVERWRITE-mode fields (and lower-companion derived columns),
         * then the merge-mode fields wrapping each with the appropriate
         * operator ({@code $ifNull} for PRESERVE_ON_NON_NULL,
         * {@code $cond}+{@code $type} for true INSERT_ONLY,
         * {@code $max} / {@code $min} for MAX / MIN). Stage order
         * matters: merge expressions reference the ORIGINAL field
         * values via {@code $field}, so they must run before any stage
         * that would overwrite those fields. We put OVERWRITE first
         * because OVERWRITE and merge field sets are disjoint by
         * construction — CodecIntrospection assigns each field exactly
         * one mode — so the merge stage's {@code $field} references
         * still see the original (pre-update) values.
         * <p>
         * Every literal value goes through {@code {$literal: v}} so
         * String values that happen to start with {@code $} aren't
         * misparsed as field-path references. Required by Mongo's
         * update-pipeline rules (see Mongo docs "Dollar Characters in
         * Field Values").
         * <p>
         * Pipeline updates have no {@code $setOnInsert}; upsert on
         * filter {@code _id = X} auto-fills {@code _id} from the
         * equality clause on insert, and {@code _id} is never set in
         * the pipeline (Mongo rejects {@code $set _id} on existing
         * docs as immutable). The codec version field {@code _v} is
         * stamped in the overwrite stage every write so the
         * schema-evolution upgrader chain on read can route by version
         * (matching the fast-path BsonCodecImpl.encode behaviour).
         */
        private void writePipeline(@NotNull Object record, @NotNull Object idValue) {
            Document overwriteDoc = new Document();
            Document mergeDoc = new Document();
            // Stamp _v explicitly. The fast RawBsonDocument path gets
            // this from BsonCodecImpl.encode; the pipeline path bypasses
            // that, so without this every merge-path first-insert would
            // produce a versionless doc.
            overwriteDoc.append(BsonCodecImpl.VERSION_FIELD, codec.version());
            int n = shape.fields().size();
            for (int i = 0; i < n; i++) {
                EncodeShape.FieldDef f = shape.fields().get(i);
                Object raw = codec.readField(record, i);
                Object bsonVal = toBsonValue(raw, f.javaType());
                switch (f.mergeMode()) {
                    case OVERWRITE -> {
                        // Skip null-value nullable refs so wire format
                        // matches the fast path (BsonCodecImpl.writeField
                        // omits null nullables rather than writing
                        // explicit BSON null). Primitive fields are
                        // never null and always emit a literal.
                        if (raw == null && f.nullable()) break;
                        overwriteDoc.append(f.name(), literal(bsonVal));
                    }
                    case PRESERVE_ON_NON_NULL -> mergeDoc.append(f.name(),
                        new Document("$ifNull", java.util.Arrays.asList("$" + f.name(), literal(bsonVal))));
                    case PRESERVE_ON_NON_SENTINEL -> {
                        // Primitive-long preserve: existing wins UNLESS
                        // existing is missing OR explicit null OR equals
                        // the declared sentinel (e.g. SessionRecord's
                        // OPEN=0L). The null branch matters because the
                        // legacy MongoBackend writes/imports open
                        // sessions as closed_at:null — without it, an
                        // incoming close timestamp from the v2 path
                        // would preserve null forever. $in over $type
                        // covers missing + null in one operand.
                        Document preserveSentinel = new Document("$cond", java.util.Arrays.asList(
                            new Document("$or", java.util.Arrays.asList(
                                new Document("$in", java.util.Arrays.asList(
                                    new Document("$type", "$" + f.name()),
                                    java.util.Arrays.asList("missing", "null"))),
                                new Document("$eq", java.util.Arrays.asList(
                                    "$" + f.name(), literal(f.sentinelValue()))))),
                            literal(bsonVal),
                            "$" + f.name()));
                        mergeDoc.append(f.name(), preserveSentinel);
                    }
                    case INSERT_ONLY -> {
                        // True insert-only: keep the existing field
                        // value if the field EXISTS on the doc at all
                        // (even if it's explicit null), else use the
                        // incoming. $type returns "missing" only when
                        // the field is absent. $ifNull would overwrite
                        // an existing null on next upsert.
                        Document insertOnly = new Document("$cond", java.util.Arrays.asList(
                            new Document("$eq", java.util.Arrays.asList(
                                new Document("$type", "$" + f.name()), "missing")),
                            literal(bsonVal),
                            "$" + f.name()));
                        mergeDoc.append(f.name(), insertOnly);
                    }
                    case MAX -> mergeDoc.append(f.name(),
                        new Document("$max", java.util.Arrays.asList("$" + f.name(), literal(bsonVal))));
                    case MIN -> mergeDoc.append(f.name(),
                        new Document("$min", java.util.Arrays.asList("$" + f.name(), literal(bsonVal))));
                }
            }
            // Lower companions sit in the overwrite stage — they're
            // derived columns refreshed on every write. Skip when the
            // source field is null/non-String (consistent with the
            // fast path).
            for (LowerCompanion lc : lowerCompanions) {
                Object raw = codec.readField(record, lc.fieldIndex);
                if (raw instanceof String s) {
                    overwriteDoc.append(lc.columnName, literal(s.toLowerCase(java.util.Locale.ROOT)));
                }
                // null source: skip entirely (matches fast path's
                // writeNull-on-null-nullable; here we'd prefer absent).
            }
            List<Bson> pipeline = new ArrayList<>(2);
            if (!overwriteDoc.isEmpty()) pipeline.add(new Document("$set", overwriteDoc));
            if (!mergeDoc.isEmpty())     pipeline.add(new Document("$set", mergeDoc));
            // updateOne(filter, pipeline, options) — Mongo 4.2+. Upsert
            // on _id-filter auto-stamps _id on insert; no $setOnInsert.
            docColl.updateOne(
                Filters.eq("_id", encodeIdValue(idValue)),
                pipeline,
                new UpdateOptions().upsert(true));
        }
    }

    /**
     * Wrap a literal value in {@code {$literal: v}} so Mongo's
     * aggregation parser doesn't interpret a String value that starts
     * with {@code $} as a field-path reference. Required for any
     * value embedded in a pipeline {@code $set} expression. Null is
     * preserved as {@code {$literal: null}} (a literal null), which
     * differs from BSON null produced by omitting the field — only
     * use when null is the intended value.
     */
    private static @NotNull Object literal(@Nullable Object v) {
        return new Document("$literal", v);
    }

    /**
     * Map a Java record field's value to a BSON-compatible value for
     * the aggregation-pipeline {@code $set} stage. Mirrors the
     * per-type write logic in {@code BsonCodecImpl.writeField}, but
     * outputs values that {@code Document.append} accepts directly.
     */
    private static @Nullable Object toBsonValue(@Nullable Object v, @NotNull Class<?> javaType) {
        if (v == null) return null;
        if (javaType == java.util.UUID.class) return BsonBinaries.uuidBinary((java.util.UUID) v);
        if (javaType == byte[].class)         return new org.bson.BsonBinary((byte[]) v);
        if (javaType.isEnum())                return ((Enum<?>) v).ordinal();
        // long/int/double/float/boolean (auto-boxed via codec.readField)
        // and String pass through unchanged — Document.append accepts
        // all of these as the BSON-typed value.
        return v;
    }

    /**
     * Resolved descriptor for one auto-populated lowercased-companion
     * field. {@code fieldIndex} is the source field's position in the
     * codec's EncodeShape; {@code columnName} is the Mongo column to
     * write (suffix {@code _lower} appended to the source name).
     */
    private record UpsertPlan(@NotNull UpsertWriter writer) {
        private static final @NotNull UpsertPlan RAW_BSON =
            new UpsertPlan((handler, record, idValue) -> handler.writeRawBson(record, idValue));
        private static final @NotNull UpsertPlan PIPELINE =
            new UpsertPlan((handler, record, idValue) -> handler.writePipeline(record, idValue));

        private static @NotNull UpsertPlan of(@NotNull EncodeShape shape) {
            for (EncodeShape.FieldDef field : shape.fields()) {
                if (field.mergeMode() != MergeMode.OVERWRITE) return PIPELINE;
            }
            return RAW_BSON;
        }

        private void write(@NotNull EntityHandler<?> handler, @NotNull Object record, @NotNull Object idValue) {
            writer.write(handler, record, idValue);
        }
    }

    @FunctionalInterface
    private interface UpsertWriter {
        void write(@NotNull EntityHandler<?> handler, @NotNull Object record, @NotNull Object idValue);
    }

    private record LowerCompanion(int fieldIndex, @NotNull String columnName) {}

    private static @NotNull LowerCompanion[] buildLowerCompanions(@NotNull Entity<?, ?, ?> kind) {
        EncodeShape shape = kind.codec().shape();
        List<LowerCompanion> out = new ArrayList<>();
        // Walk every caseInsensitivePrefix=true index; for each of its
        // declared fields, resolve to a persistent-field index and stamp
        // a companion descriptor. The set is small in practice (Phase 3b
        // ships exactly one: PlayerIdentity.by_name → current_name_lower).
        for (IndexSpec spec : kind.secondaryIndexes()) {
            if (!spec.caseInsensitivePrefix()) continue;
            for (String f : spec.fields()) {
                String fieldName = stripDir(f);
                int idx = resolveFieldIndex(shape, fieldName);
                if (idx < 0) {
                    throw new IllegalStateException("caseInsensitivePrefix index '" + spec.name()
                        + "' references unknown field '" + fieldName + "' on " + kind.recordType().getName());
                }
                out.add(new LowerCompanion(idx, fieldName + "_lower"));
            }
        }
        return out.toArray(new LowerCompanion[0]);
    }

    private static int resolveFieldIndex(@NotNull EncodeShape shape, @NotNull String fieldName) {
        List<EncodeShape.FieldDef> fields = shape.fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(fieldName)) return i;
        }
        return -1;
    }

    // ============================== execute dispatch ==============================

    private <ID, R> Optional<R> getById(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull EntityOps.GetByIdOp<ID, R> op) {
        Document doc = docColl(id).find(Filters.eq("_id", encodeIdValue(op.id()))).first();
        if (doc == null) return Optional.empty();
        @SuppressWarnings({"rawtypes", "unchecked"})
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        @SuppressWarnings("unchecked")
        R r = (R) codec.decode(projectIdIfMissing(doc, kind.codec().shape()));
        return Optional.of(r);
    }

    /**
     * Defensive decode glue: if {@code shape.idField()} isn't present
     * on the doc but {@code _id} is, alias the value before handing
     * to the codec. Catches reads against legacy rows that haven't
     * yet been touched by
     * {@link MongoEntityV6ToV7UuidSubtypeMigration} — those rows have
     * the @Id value ONLY in {@code _id}, and the codec's
     * missing-non-nullable check would otherwise throw.
     * <p>
     * <strong>NOT a substitute for the migration.</strong> An
     * unmigrated row still has subtype-0 {@code _id} and any
     * {@code findOne(_id == subtype-4-uuid)} query MISSES the row
     * before this glue ever runs. The migration is what fixes the
     * query-side impedance; this helper just keeps the decode-side
     * working once a row is reached by some other means (full scan,
     * pre-migration introspection, etc.).
     */
    private static @NotNull Document projectIdIfMissing(@NotNull Document doc,
                                                        @NotNull EncodeShape shape) {
        String idName = shape.idField();
        if (idName.equals("_id")) return doc;
        if (doc.containsKey(idName)) return doc;
        Object id = doc.get("_id");
        if (id == null) return doc;
        Document copy = new Document(doc);
        copy.put(idName, id);
        return copy;
    }

    private <ID, R> List<R> getMany(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull EntityOps.GetManyOp<ID, R> op) {
        if (op.ids().isEmpty()) return List.of();
        List<Object> encodedIds = new ArrayList<>(op.ids().size());
        for (ID i : op.ids()) encodedIds.add(encodeIdValue(i));
        @SuppressWarnings({"rawtypes", "unchecked"})
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        List<R> out = new ArrayList<>(op.ids().size());
        EncodeShape shape = kind.codec().shape();
        for (Document d : docColl(id).find(Filters.in("_id", encodedIds))) {
            @SuppressWarnings("unchecked")
            R r = (R) codec.decode(projectIdIfMissing(d, shape));
            out.add(r);
        }
        return out;
    }

    private <R> Page<R> findByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull EntityOps.FindByIndexOp<R> op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        IndexSpec effective = caseInsensitiveEffective(spec);
        // Equality fixes the leading column; cursor must seek on the NEXT
        // column down (or fall back to _id-only seek if this is a single-
        // column index).
        Object key = spec.caseInsensitivePrefix() && op.key() instanceof String s
            ? s.toLowerCase(java.util.Locale.ROOT)
            : op.key();
        Bson filter = Filters.eq(stripDir(effective.fields().get(0)), encodeIndexValue(key));
        return pageRead(id, kind, filter, effective, /*equalityColumnIndex=*/0, op.cursor(), op.pageSize());
    }

    private <R> Page<R> prefixIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull EntityOps.PrefixIndexOp<R> op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        // For caseInsensitivePrefix indexes, the actual scan target is
        // the <field>_lower companion populated by EntityHandler — both
        // the filter, the sort, and the cursor ordered column must use
        // the lower companion so Mongo picks the matching index and the
        // pagination compares lowercased-against-lowercased. The original
        // field stays untouched, preserving display-case on the row.
        IndexSpec effective = caseInsensitiveEffective(spec);
        String field = stripDir(effective.fields().get(0));
        String prefix = spec.caseInsensitivePrefix()
            ? op.prefix().toLowerCase(java.util.Locale.ROOT)
            : op.prefix();
        // Anchored regex; quoted to escape any regex metas in the prefix.
        String pattern = "^" + java.util.regex.Pattern.quote(prefix);
        Bson filter = Filters.regex(field, pattern, "");
        // Range scan on the leading column — there is no equality-fixed
        // column, so cursor ordering uses field 0 directly.
        return pageRead(id, kind, filter, effective, /*equalityColumnIndex=*/-1, op.cursor(), op.pageSize());
    }

    /**
     * Rewrite an {@link IndexSpec}'s leading column to its
     * {@code _lower} companion if {@code caseInsensitivePrefix} is set.
     * Used for query-side routing of {@code prefixIndex}: scans, sorts,
     * and cursors target the lower companion (matching the index
     * created at {@code ensureStore} and the value written by
     * {@code EntityHandler}). Returns the spec unchanged when
     * {@code caseInsensitivePrefix} is false.
     */
    private static @NotNull IndexSpec caseInsensitiveEffective(@NotNull IndexSpec spec) {
        if (!spec.caseInsensitivePrefix()) return spec;
        List<String> fields = spec.fields();
        if (fields.isEmpty()) return spec;
        List<String> rewritten = new ArrayList<>(fields);
        String leading = rewritten.get(0);
        boolean desc = leading.startsWith("-");
        String name = desc ? leading.substring(1) : leading;
        rewritten.set(0, (desc ? "-" : "") + name + "_lower");
        return new IndexSpec(spec.name(), List.copyOf(rewritten), spec.unique(), spec.caseInsensitivePrefix());
    }

    private long countByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind, @NotNull EntityOps.CountByIndexOp op) {
        // Resolve the index spec so we count on the index's leading column
        // (stripped of direction prefix), not on the literal indexName the
        // caller passed.
        IndexSpec spec = requireIndex(kind, op.indexName());
        String leading = stripDir(spec.fields().get(0));
        return docColl(id).countDocuments(Filters.eq(leading, encodeIndexValue(op.key())));
    }

    private <ID> void deleteById(@NotNull StoreId id, @NotNull EntityOps.DeleteByIdOp<ID> op) {
        docColl(id).deleteOne(Filters.eq("_id", encodeIdValue(op.id())));
    }

    private void deleteByIndex(@NotNull StoreId id, @NotNull Entity<?, ?, ?> kind,
                               @NotNull EntityOps.DeleteByIndexOp op) {
        IndexSpec spec = requireIndex(kind, op.indexName());
        // Leading column equality — same contract as FindByIndexOp.
        // Multi-column indexes filter on the first declared field only.
        Bson filter = Filters.eq(stripDir(spec.fields().get(0)), encodeIndexValue(op.key()));
        docColl(id).deleteMany(filter);
    }

    private <R> Page<R> pageRead(
            @NotNull StoreId id,
            @NotNull Entity<?, ?, ?> kind,
            @NotNull Bson filter,
            @NotNull IndexSpec spec,
            int equalityColumnIndex,
            @Nullable Cursor cursor,
            int pageSize) {
        int ps = Math.max(1, pageSize);
        @SuppressWarnings({"rawtypes", "unchecked"})
        BsonCodec codec = BsonCodecs.regular(kind.recordType());
        Bson finalFilter = applyCursor(filter, spec, equalityColumnIndex, cursor);
        List<Document> docs = new ArrayList<>(ps + 1);
        Bson sort = buildSort(spec);
        docColl(id).find(finalFilter).sort(sort).limit(ps + 1).into(docs);
        boolean hasMore = docs.size() > ps;
        List<R> items = new ArrayList<>(Math.min(docs.size(), ps));
        Document last = null;
        int n = Math.min(docs.size(), ps);
        EncodeShape shape = kind.codec().shape();
        for (int i = 0; i < n; i++) {
            Document d = docs.get(i);
            @SuppressWarnings("unchecked")
            R r = (R) codec.decode(projectIdIfMissing(d, shape));
            items.add(r);
            last = d;
        }
        Cursor next = (hasMore && last != null) ? encodeCursor(last, spec, equalityColumnIndex) : null;
        return new Page<>(items, next);
    }

    private static @NotNull Bson buildSort(@NotNull IndexSpec spec) {
        Document sort = new Document();
        for (String f : spec.fields()) {
            sort.append(stripDir(f), f.startsWith("-") ? -1 : 1);
        }
        // Always tie-break by _id so paging is deterministic.
        sort.append("_id", 1);
        return sort;
    }

    /**
     * Resolves the cursor's ordered-column position within the index. The
     * ordered column is the first index field that ISN'T pinned to an
     * equality value by the operation. For a single-column index used by
     * FindByIndex, every column is pinned, so there is no ordered column
     * and pagination falls back to _id-only seek. For a compound
     * {@code (player_uuid, -started_at)} index queried with
     * {@code FindByIndex(player_uuid=...)}, equalityColumnIndex=0 and the
     * ordered column is {@code -started_at} (index 1).
     *
     * @return position of the ordered column in {@link IndexSpec#fields()},
     *         or -1 if no column is ordered (equality pins them all).
     */
    private static int orderedColumnIndex(@NotNull IndexSpec spec, int equalityColumnIndex) {
        int next = equalityColumnIndex + 1;
        return next < spec.fields().size() ? next : -1;
    }

    private static @NotNull Cursor encodeCursor(@NotNull Document last, @NotNull IndexSpec spec, int equalityColumnIndex) {
        int orderedIdx = orderedColumnIndex(spec, equalityColumnIndex);
        byte[] idBytes = idAsBytes(last.get("_id"));
        if (orderedIdx < 0) {
            // No ordered column — pagination seeks purely on _id. Stamp a
            // 0 numeric ordered key so applyCursor can detect id-only seek.
            return Cursors.encode(0L, idBytes);
        }
        String orderedField = stripDir(spec.fields().get(orderedIdx));
        Object orderedRaw = last.get(orderedField);
        // Per-type encode. Integers (Long, Integer, Short, Byte) route
        // through the compact long cursor; Double/Float, Date, String,
        // Binary each use their own typed-cursor branch so the next-page
        // filter compares the right BSON type.
        if (orderedRaw instanceof Long || orderedRaw instanceof Integer
                || orderedRaw instanceof Short || orderedRaw instanceof Byte) {
            return Cursors.encode(((Number) orderedRaw).longValue(), idBytes);
        }
        if (orderedRaw instanceof Double || orderedRaw instanceof Float) {
            double d = ((Number) orderedRaw).doubleValue();
            byte[] payload = new byte[8];
            java.nio.ByteBuffer.wrap(payload).putDouble(d);
            return Cursors.encodeTyped(Cursors.TYPE_DOUBLE, payload, idBytes);
        }
        if (orderedRaw instanceof java.util.Date date) {
            byte[] payload = new byte[8];
            java.nio.ByteBuffer.wrap(payload).putLong(date.getTime());
            return Cursors.encodeTyped(Cursors.TYPE_DATE, payload, idBytes);
        }
        if (orderedRaw instanceof String s) {
            return Cursors.encodeTyped(Cursors.TYPE_STRING,
                s.getBytes(java.nio.charset.StandardCharsets.UTF_8), idBytes);
        }
        if (orderedRaw instanceof byte[] b) {
            return Cursors.encodeTyped(Cursors.TYPE_BINARY,
                packBinary(org.bson.BsonBinarySubType.BINARY.getValue(), b), idBytes);
        }
        if (orderedRaw instanceof org.bson.types.Binary b) {
            return Cursors.encodeTyped(Cursors.TYPE_BINARY,
                packBinary(b.getType(), b.getData()), idBytes);
        }
        if (orderedRaw instanceof org.bson.BsonBinary b) {
            return Cursors.encodeTyped(Cursors.TYPE_BINARY,
                packBinary(b.getType(), b.getData()), idBytes);
        }
        // Final fallback: stringify. Used for unusual BSON types
        // (regex, decimal128, etc.) that aren't expected on indexed
        // columns. Loses no fidelity for the types we expect in practice
        // and produces deterministic cursor strings for stability.
        return Cursors.encodeTyped(Cursors.TYPE_STRING,
            String.valueOf(orderedRaw).getBytes(java.nio.charset.StandardCharsets.UTF_8), idBytes);
    }

    private static byte @NotNull [] packBinary(byte subtype, byte @NotNull [] data) {
        byte[] out = new byte[1 + data.length];
        out[0] = subtype;
        System.arraycopy(data, 0, out, 1, data.length);
        return out;
    }

    private @NotNull Bson applyCursor(
            @NotNull Bson base,
            @NotNull IndexSpec spec,
            int equalityColumnIndex,
            @Nullable Cursor cursor) {
        if (cursor == null) return base;
        int orderedIdx = orderedColumnIndex(spec, equalityColumnIndex);
        byte schema = Cursors.peekSchema(cursor);
        if (schema == Cursors.SCHEMA_ORDERED_PAIR) {
            return applyNumericCursor(base, spec, orderedIdx, Cursors.decode(cursor));
        }
        if (schema == Cursors.SCHEMA_TYPED_PAIR) {
            return applyTypedCursor(base, spec, orderedIdx, Cursors.decodeTyped(cursor));
        }
        throw new IllegalArgumentException("unsupported cursor schema 0x" + Integer.toHexString(schema & 0xff));
    }

    private static @NotNull Bson applyNumericCursor(
            @NotNull Bson base,
            @NotNull IndexSpec spec,
            int orderedIdx,
            @NotNull Cursors.Decoded d) {
        org.bson.BsonBinary idBin = wrapBinaryIfUuid(d.idBytes());
        if (orderedIdx < 0) {
            // No ordered column — equality pins every field. Seek on _id
            // alone; sort tie-breaker on _id keeps pagination deterministic.
            return Filters.and(base, Filters.gt("_id", idBin));
        }
        String orderedField = stripDir(spec.fields().get(orderedIdx));
        boolean descending = spec.fields().get(orderedIdx).startsWith("-");
        Bson orderedPast = descending
            ? Filters.lt(orderedField, d.orderedKey())
            : Filters.gt(orderedField, d.orderedKey());
        Bson tiedThenIdPast = Filters.and(
            Filters.eq(orderedField, d.orderedKey()),
            Filters.gt("_id", idBin));
        return Filters.and(base, Filters.or(orderedPast, tiedThenIdPast));
    }

    private static @NotNull Bson applyTypedCursor(
            @NotNull Bson base,
            @NotNull IndexSpec spec,
            int orderedIdx,
            @NotNull Cursors.DecodedTyped d) {
        org.bson.BsonBinary idBin = wrapBinaryIfUuid(d.idBytes());
        if (orderedIdx < 0) {
            // The encoder never produces a typed cursor when there's no
            // ordered column, but be defensive on decode.
            return Filters.and(base, Filters.gt("_id", idBin));
        }
        String orderedField = stripDir(spec.fields().get(orderedIdx));
        boolean descending = spec.fields().get(orderedIdx).startsWith("-");
        Object orderedValue = decodeTypedOrderedValue(d.typeTag(), d.orderedBytes());
        Bson orderedPast = descending
            ? Filters.lt(orderedField, orderedValue)
            : Filters.gt(orderedField, orderedValue);
        Bson tiedThenIdPast = Filters.and(
            Filters.eq(orderedField, orderedValue),
            Filters.gt("_id", idBin));
        return Filters.and(base, Filters.or(orderedPast, tiedThenIdPast));
    }

    private static @NotNull Object decodeTypedOrderedValue(byte typeTag, byte @NotNull [] bytes) {
        return switch (typeTag) {
            case Cursors.TYPE_STRING -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            case Cursors.TYPE_BINARY -> {
                // Payload format: [1 byte subtype][N bytes data]. Preserves
                // the BSON binary subtype so UUID_STANDARD (0x04) cursors
                // compare against the stored BsonBinary with matching
                // subtype — subtype participates in BSON binary ordering.
                if (bytes.length < 1) {
                    throw new IllegalArgumentException("BINARY typed cursor payload missing subtype byte");
                }
                byte subtype = bytes[0];
                byte[] data = new byte[bytes.length - 1];
                System.arraycopy(bytes, 1, data, 0, data.length);
                // BsonBinary(byte, byte[]) takes the raw subtype byte
                // directly — no enum lookup needed, and any vendor-defined
                // subtypes round-trip without translation.
                yield new org.bson.BsonBinary(subtype, data);
            }
            case Cursors.TYPE_DOUBLE -> {
                if (bytes.length != 8) {
                    throw new IllegalArgumentException("DOUBLE typed cursor payload must be 8 bytes, got " + bytes.length);
                }
                yield java.nio.ByteBuffer.wrap(bytes).getDouble();
            }
            case Cursors.TYPE_DATE -> {
                if (bytes.length != 8) {
                    throw new IllegalArgumentException("DATE typed cursor payload must be 8 bytes, got " + bytes.length);
                }
                // Reconstruct java.util.Date so Mongo's filter sees a Date
                // value and compares Date-to-Date (it will not coerce a
                // long to Date for range filters).
                yield new java.util.Date(java.nio.ByteBuffer.wrap(bytes).getLong());
            }
            default -> throw new IllegalArgumentException("unsupported typed-cursor type tag 0x" + Integer.toHexString(typeTag & 0xff));
        };
    }

    // ============================== id + index encoding ==============================

    private static @NotNull Object encodeIdValue(@NotNull Object id) {
        if (id instanceof java.util.UUID u) return BsonBinaries.uuidBinary(u);
        if (id instanceof Enum<?> e)        return e.ordinal();
        if (id instanceof byte[] b)         return new org.bson.BsonBinary(b);
        return id;
    }

    private static @NotNull Object encodeIndexValue(@NotNull Object v) {
        // Mirror the codec's per-type encoding so equality filters match
        // what was actually written.
        if (v instanceof java.util.UUID u) return BsonBinaries.uuidBinary(u);
        if (v instanceof Enum<?> e)        return e.ordinal();
        if (v instanceof byte[] b)         return new org.bson.BsonBinary(b);
        return v;
    }

    private static @NotNull org.bson.BsonBinary wrapBinaryIfUuid(byte @NotNull [] idBytes) {
        return idBytes.length == 16
            ? new org.bson.BsonBinary(org.bson.BsonBinarySubType.UUID_STANDARD, idBytes)
            : new org.bson.BsonBinary(idBytes);
    }

    private static byte @NotNull [] idAsBytes(@NotNull Object raw) {
        if (raw instanceof byte[] b) return b;
        if (raw instanceof org.bson.types.Binary b) return b.getData();
        if (raw instanceof org.bson.BsonBinary b)  return b.getData();
        // Non-binary id (e.g. string) — use UTF-8 bytes for cursor stability.
        return raw.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    // ============================== misc helpers ==============================

    private @NotNull MongoCollection<Document> docColl(@NotNull StoreId id) {
        return docCollections.computeIfAbsent(id.qualified(), k -> db.getCollection(id.name()));
    }

    private @NotNull MongoCollection<RawBsonDocument> rawCollection(@NotNull StoreId id) {
        return rawCollections.computeIfAbsent(id.qualified(),
            k -> db.getCollection(id.name(), RawBsonDocument.class));
    }

    private static @NotNull IndexSpec requireIndex(@NotNull Entity<?, ?, ?> kind, @NotNull String name) {
        for (IndexSpec spec : kind.secondaryIndexes()) {
            if (spec.name().equals(name)) return spec;
        }
        throw new IllegalArgumentException(
            "no secondary index named '" + name + "' on Entity " + kind.name());
    }
}
