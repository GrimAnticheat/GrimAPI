package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.CapturedBindings;
import ac.grim.grimac.internal.storage.codec.CodecIntrospection;
import ac.grim.grimac.internal.storage.codec.RecordLayout;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import ac.grim.grimac.internal.storage.codec.bson.BsonCodecs;
import ac.grim.grimac.internal.storage.codec.bson.BsonTsCodec;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Migrate {@code grim_violations} from the v6 layout (regular collection,
 * flat scalar fields, snake_case names) to the v7 layout (Mongo timeseries
 * collection, partition fields nested under {@code meta}, timestamp as BSON
 * Date, {@code _v} marker).
 *
 * <p><strong>Single-rename pattern.</strong> The naive two-rename swap
 * (canonical → backup, staging → canonical) doesn't work for timeseries
 * because Mongo implements timeseries as a view + bucket pair, and
 * {@code renameCollection} is forbidden on views (error 166). Instead:
 *
 * <ol>
 *   <li>If canonical is already timeseries:
 *     <ul>
 *       <li>{@code canonical.count == backup.count > 0} → migration done;
 *           re-apply TTL (idempotent) and return.</li>
 *       <li>{@code canonical.count < backup.count} → prior partial copy;
 *           drop canonical + recreate as timeseries + re-copy from backup.</li>
 *       <li>{@code canonical.count > backup.count} → migration completed
 *           earlier and new v7 rows were written afterward. Preserve the
 *           backup, re-apply TTL, and continue.</li>
 *       <li>No backup → clean v2-only state; skip.</li>
 *     </ul>
 *   </li>
 *   <li>If only backup exists (we crashed between rename and create) →
 *       recreate canonical as timeseries, resume copy.</li>
 *   <li>Otherwise (canonical is a regular v6 collection):
 *     <ol type="a">
 *       <li>Rename canonical → {@code <name>_v6_bak} (allowed, regular
 *           collection). Throws if an existing backup is found —
 *           silently dropping it could destroy an orphan from a prior
 *           successful migration the operator has since reset.</li>
 *       <li>Create canonical AS timeseries (no staging step).</li>
 *       <li>Stream-copy from backup → canonical via the EventStream's
 *           {@link BsonTsCodec}.</li>
 *       <li>Verify counts; apply TTL via {@code collMod}.</li>
 *     </ol>
 *   </li>
 * </ol>
 *
 * <p>Crash safety: between (a) and (d), the v6 data is intact at
 * {@code _v6_bak}. A crash mid-copy leaves canonical with fewer rows
 * than backup; the next run's count-mismatch detection (branch 1)
 * drops canonical and re-copies.
 *
 * <p>Resumable: every state above leads to a self-healing path on
 * re-run, no operator intervention required as long as either
 * canonical or backup still exists with the source data.
 */
@ApiStatus.Internal
public final class MongoViolationsV6ToV7Migration implements Migration<EventStream<?, ?>> {

    private static final int BATCH = 5_000;
    private static final String STAGING_SUFFIX = "_v7_migrating";
    private static final String BACKUP_SUFFIX  = "_v6_bak";

    @Override public int fromVersion() { return 0; }    // pre-v7 baseline
    @Override public int toVersion()   { return 1; }    // first v7-shaped version of the store

    @Override
    public void apply(@NotNull MigrationContext ctx, @NotNull StoreId id, @NotNull EventStream<?, ?> kind) throws Exception {
        if (!(ctx instanceof MongoMigrationContext mctx)) {
            throw new IllegalArgumentException(
                "MongoViolationsV6ToV7Migration requires a MongoMigrationContext; got " + ctx.getClass().getName());
        }
        run(mctx.database(), mctx.logger(), id, kind);
    }

    private void run(
            @NotNull MongoDatabase db,
            @NotNull Logger logger,
            @NotNull StoreId id,
            @NotNull EventStream<?, ?> kind) throws Exception {

        String canonical = id.name();
        String backup    = canonical + BACKUP_SUFFIX;

        // Mongo timeseries collections are implemented as a view +
        // backing system.buckets.<name> collection. renameCollection
        // is forbidden on views (error 166 CommandNotSupportedOnView),
        // so the previous two-rename atomic swap pattern crashed
        // mid-flight every time the target was timeseries. New flow:
        //
        //   1. If canonical is already timeseries → done (idempotent).
        //   2. If only _v6_bak exists (canonical dropped post-rename
        //      pre-create): recreate canonical as timeseries, resume.
        //   3. Otherwise:
        //      a. Rename canonical (regular) → _v6_bak (allowed,
        //         it IS still a regular collection at this point).
        //      b. Create canonical AS timeseries (no staging).
        //      c. Stream-copy from _v6_bak → canonical.
        //      d. Verify counts; apply TTL via collMod.
        //
        // Resume semantics: a crash between (b) and (c) leaves canonical
        // empty-timeseries + _v6_bak full. The (2) branch detects this
        // by canonical existing but empty and re-copies.

        // (1) Already migrated? Three sub-cases — only the third
        // (counts match exactly) is "skip"; the others fall through
        // to a clean re-copy because a partial / interrupted copy
        // left the canonical with fewer rows than the backup AND no
        // durable completion marker tells us where we left off.
        // Standalone Mongo has no atomic completion semaphore we can
        // rely on; we use canonical_count == backup_count as proof
        // that the copy reached the end.
        if (isTimeseriesCollection(db, canonical)) {
            long tsCount = db.getCollection(canonical).countDocuments();
            boolean hasBackup = collectionExists(db, backup);
            long bakCount = hasBackup ? db.getCollection(backup).countDocuments() : -1L;

            if (hasBackup && tsCount == bakCount && bakCount > 0) {
                // Completion-proven: counts match → skip. Re-apply TTL
                // as a no-op in case a prior run crashed between copy
                // and collMod (collMod is idempotent).
                logger.fine(() -> canonical + " already migrated (" + bakCount
                    + " rows match between canonical timeseries and " + backup + "); ensuring TTL");
                applyTtl(db, canonical, kind);
                return;
            }
            if (hasBackup && bakCount > 0) {
                // Mismatch. Distinguish UNDER-copy (tsCount < bakCount)
                // from OVER-copy (tsCount > bakCount):
                //
                // tsCount < bakCount: partial copy from a crashed run.
                // Drop canonical and re-copy from backup. Safe — the
                // missing rows live in backup and copy is idempotent.
                //
                // tsCount > bakCount: the copy already completed and new
                // v7 rows landed in canonical afterward. This is the normal
                // state once we preserve _v6_bak for operator rollback and
                // then boot again after live traffic. Do not drop canonical
                // and do not block route registration.
                if (tsCount > bakCount) {
                    logger.warning(() -> canonical + " is timeseries with " + tsCount + " rows while "
                        + backup + " has " + bakCount + " — treating as already migrated with "
                        + "post-migration writes; preserving backup and ensuring TTL");
                    applyTtl(db, canonical, kind);
                    return;
                }
                logger.warning(() -> canonical + " is timeseries with " + tsCount + " rows but "
                    + backup + " has " + bakCount + " — partial copy detected; re-copying from backup");
                db.getCollection(canonical).drop();
                createTimeseries(db, canonical, kind);
                streamCopy(db, backup, canonical, kind, logger);
                applyTtl(db, canonical, kind);
                return;
            }
            // canonical IS timeseries but no backup. Either a clean
            // v2-only deployment (no legacy data ever existed) or the
            // operator manually dropped backup. Either way nothing to
            // migrate.
            logger.fine(() -> canonical + " is already a timeseries collection (no backup); migration skipped");
            return;
        }

        // (2) Recovery: canonical absent, _v6_bak present. We crashed
        // between rename-to-backup and create-as-timeseries. Recreate
        // canonical as timeseries and copy.
        if (!collectionExists(db, canonical) && collectionExists(db, backup)) {
            long bakCount = db.getCollection(backup).countDocuments();
            logger.warning(() -> "recovering: canonical missing, " + backup + " has " + bakCount
                + " rows. Recreating " + canonical + " as timeseries and copying.");
            createTimeseries(db, canonical, kind);
            streamCopy(db, backup, canonical, kind, logger);
            applyTtl(db, canonical, kind);
            return;
        }

        if (!collectionExists(db, canonical)) {
            logger.fine(() -> canonical + " does not exist; nothing to migrate");
            return;
        }

        logger.info(() -> "migrating " + canonical + " from v6 → v7 (timeseries)");

        // (3a) Move legacy aside. Allowed because canonical is regular.
        // We DO NOT silently drop an existing backup here — it could be
        // an orphan from a prior successful migration whose canonical
        // the operator later recreated as v6 (or re-imported v6 data),
        // and rolling canonical into backup would destroy the orphan's
        // data without recovery. Throw and demand operator review.
        if (collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            long canonicalCount = db.getCollection(canonical).countDocuments();
            throw new IllegalStateException(
                "refusing to overwrite existing " + backup + " (rows=" + backupCount
                    + ") while migrating non-empty canonical " + canonical
                    + " (rows=" + canonicalCount + "); both contain v6-shaped data and"
                    + " cannot be safely reconciled. Manual operator review required"
                    + " (decide which v6 source is authoritative, drop the other, then"
                    + " rerun migration).");
        }
        db.getCollection(canonical).renameCollection(
            new MongoNamespace(db.getName(), backup), new RenameCollectionOptions());

        // (3b) Create canonical as timeseries.
        createTimeseries(db, canonical, kind);

        // (3c) Stream-copy + (3d) verify + apply TTL.
        streamCopy(db, backup, canonical, kind, logger);
        applyTtl(db, canonical, kind);

        logger.info(() -> "migration complete: "
            + db.getCollection(canonical).countDocuments()
            + " docs in timeseries " + canonical + "; v6 collection preserved at " + backup);
    }

    private void createTimeseries(@NotNull MongoDatabase db, @NotNull String name,
                                  @NotNull EventStream<?, ?> kind) {
        TimeSeriesOptions tsOpts = new TimeSeriesOptions(kind.timestampField())
            .metaField("meta")
            .granularity(toMongoGranularity(kind));
        // Don't apply TTL here — TTL monitor could delete rows
        // mid-copy whose timestamp falls outside retention. collMod
        // after copy completes.
        db.createCollection(name, new CreateCollectionOptions().timeSeriesOptions(tsOpts));
    }

    private void streamCopy(@NotNull MongoDatabase db, @NotNull String fromName, @NotNull String toName,
                            @NotNull EventStream<?, ?> kind, @NotNull Logger logger) throws Exception {
        MongoCollection<Document> src = db.getCollection(fromName);
        MongoCollection<RawBsonDocument> dst = db.getCollection(toName, RawBsonDocument.class);
        @SuppressWarnings({"rawtypes", "unchecked"})
        BsonTsCodec codec = BsonCodecs.timeseries(kind.recordType());

        long total = src.countDocuments();
        long copied = 0;
        BasicOutputBuffer buf = new BasicOutputBuffer(256);
        List<RawBsonDocument> pending = new ArrayList<>(BATCH);

        for (Document v6 : src.find().noCursorTimeout(true).batchSize(BATCH)) {
            Object record = decodeV6Doc(v6, kind);
            buf.truncateToPosition(0);
            BsonBinaryWriter writer = new BsonBinaryWriter(buf);
            try {
                @SuppressWarnings("unchecked")
                BsonTsCodec untyped = (BsonTsCodec) codec;
                untyped.encode(record, writer);
            } finally {
                writer.close();
            }
            pending.add(new RawBsonDocument(buf.toByteArray()));
            if (pending.size() >= BATCH) {
                dst.insertMany(pending);
                copied += pending.size();
                pending.clear();
                long pct = total > 0 ? (copied * 100 / total) : 0;
                logger.fine(() -> "migrated " + pct + "%");
            }
        }
        if (!pending.isEmpty()) {
            dst.insertMany(pending);
            copied += pending.size();
        }

        long destCount = dst.countDocuments();
        if (destCount != copied) {
            throw new IllegalStateException(
                "post-copy count mismatch: copied=" + copied + " in_dest=" + destCount);
        }
        long sourceCountFinal = src.countDocuments();
        // Offline-only: any source/dest divergence means live writes
        // happened during the migration window. The operator must
        // stop legacy writers + re-run.
        if (sourceCountFinal != copied) {
            throw new IllegalStateException(
                "source/destination divergence: source=" + sourceCountFinal + " copied=" + copied
                    + " — re-run migration after stopping live writes, or enable online mode (Phase 3+).");
        }
    }

    private void applyTtl(@NotNull MongoDatabase db, @NotNull String name,
                          @NotNull EventStream<?, ?> kind) {
        if (kind.retention() == null) return;
        db.runCommand(new Document()
            .append("collMod", name)
            .append("expireAfterSeconds", kind.retention().toSeconds()));
    }

    // ============================== helpers ==============================

    private static boolean collectionExists(@NotNull MongoDatabase db, @NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    /** Mongo exposes timeseries info via {@code listCollections} {@code options.timeseries}. */
    private static boolean isTimeseriesCollection(@NotNull MongoDatabase db, @NotNull String name) {
        try {
            for (Document collInfo : db.listCollections().filter(new Document("name", name))) {
                Document options = collInfo.get("options", Document.class);
                if (options != null && options.containsKey("timeseries")) return true;
                String type = collInfo.getString("type");
                if ("timeseries".equals(type)) return true;
            }
        } catch (RuntimeException ignored) {
            // listCollections can fail on some auth configs; treat as "unknown" → re-attempt migration.
        }
        return false;
    }

    private static @NotNull TimeSeriesGranularity toMongoGranularity(@NotNull EventStream<?, ?> kind) {
        return switch (kind.granularity()) {
            case SECONDS -> TimeSeriesGranularity.SECONDS;
            case MINUTES -> TimeSeriesGranularity.MINUTES;
            case HOURS   -> TimeSeriesGranularity.HOURS;
        };
    }

    /**
     * Decode a v6 Document (flat, snake_case scalars) into the record
     * shape the v7 codec expects. The v6 violations collection is the only
     * caller of this method today; Entity/KVScoped variants of the migration
     * will ship in their own classes.
     * <p>
     * Reads the encoded field names from the codec's {@link EncodeShape}
     * so {@code @Name("occurred_at")} overrides are honoured — without
     * this, the v6 doc's {@code occurred_at} would be missed
     * (snake_case of {@code occurredEpochMs} is {@code occurred_epoch_ms}),
     * producing silently-corrupt migrated rows with timestamps stuck at
     * {@code Date(0)}.
     */
    private static @NotNull Object decodeV6Doc(@NotNull Document v6, @NotNull EventStream<?, ?> kind) {
        try {
            Class<?> rec = kind.recordType();
            EncodeShape shape = CodecIntrospection.inspect(rec);
            RecordLayout layout = CapturedBindings.layout(rec);
            if (layout == null) layout = RecordLayout.fromReflection(rec, shape);
            Class<?>[] paramTypes = layout.ctorParamTypes();

            // Full-arity arg array; persistent fields placed by recordIndex,
            // transient slots left at their Java default. The v6 violations
            // record has no transients (Entity/KVScoped migrations ship in
            // their own classes), so every slot is written below.
            Object[] args = new Object[paramTypes.length];
            for (EncodeShape.FieldDef f : shape.fields()) {
                int pos = f.recordIndex();
                String encoded = f.name();
                args[pos] = coerceV6Value(v6, v6.get(encoded), paramTypes[pos], encoded);
            }

            // Bind the canonical constructor by param types — no RecordComponents
            // (Allatori strips that attribute; the captured layout carries the types).
            MethodHandle ctor = MethodHandles.privateLookupIn(rec, MethodHandles.lookup())
                .findConstructor(rec, MethodType.methodType(void.class, paramTypes));
            return ctor.invokeWithArguments(args);
        } catch (Throwable t) {
            throw new IllegalStateException("v6 decode failed: " + v6, t);
        }
    }

    private static Object coerceV6Value(@NotNull Document v6, Object raw, @NotNull Class<?> javaType, @NotNull String encoded) {
        if (raw == null) {
            if (javaType.isPrimitive()) {
                if (javaType == int.class) return 0;
                if (javaType == long.class) return 0L;
                if (javaType == double.class) return 0d;
                if (javaType == float.class) return 0f;
                if (javaType == boolean.class) return false;
                throw new IllegalStateException("unsupported primitive " + javaType + " in v6 field " + encoded);
            }
            return null;
        }
        if (javaType == int.class || javaType == Integer.class) return ((Number) raw).intValue();
        if (javaType == long.class || javaType == Long.class) {
            if (raw instanceof java.util.Date d) return d.getTime();
            return ((Number) raw).longValue();
        }
        if (javaType == double.class || javaType == Double.class) return ((Number) raw).doubleValue();
        if (javaType == float.class || javaType == Float.class) return ((Number) raw).floatValue();
        if (javaType == boolean.class || javaType == Boolean.class) return raw;
        if (javaType == String.class) return raw.toString();
        if (javaType == UUID.class) return BsonBinaries.toUuid(raw);
        if (javaType == byte[].class) {
            if (raw instanceof byte[] b) return b;
            if (raw instanceof org.bson.types.Binary b) return b.getData();
            if (raw instanceof org.bson.BsonBinary b) return b.getData();
            if ("verbose".equals(encoded) && raw instanceof String s) {
                Object rawFormat = v6.get("verbose_format");
                int format = rawFormat instanceof Number n ? n.intValue() : 0;
                if (format == 1) return Base64.getDecoder().decode(s);
                return s.getBytes(StandardCharsets.UTF_8);
            }
            throw new IllegalStateException("v6 " + encoded + ": unexpected binary type " + raw.getClass().getName());
        }
        if (javaType.isEnum()) {
            int ordinal = ((Number) raw).intValue();
            Object[] values = javaType.getEnumConstants();
            if (ordinal < 0 || ordinal >= values.length) {
                throw new IllegalStateException("v6 " + encoded + ": enum ordinal " + ordinal + " out of range");
            }
            return values[ordinal];
        }
        throw new IllegalStateException("v6 " + encoded + ": unsupported java type " + javaType.getName());
    }
}
