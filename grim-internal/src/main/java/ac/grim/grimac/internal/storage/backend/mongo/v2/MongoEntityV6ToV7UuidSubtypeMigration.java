package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.IndexSpec;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.RenameCollectionOptions;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * v6 → v7 UUID-subtype + idField projection for a UUID-keyed Entity
 * Mongo collection.
 *
 * <p>Two impedances exist between the legacy {@code MongoBackend}
 * write path and the v2 {@code MongoEntityAdapter} read path:
 * <ol>
 *   <li><strong>BsonBinary subtype.</strong> Legacy writes UUIDs via
 *       {@code new BsonBinary(byte[])} → subtype 0 (generic). v2 writes
 *       and queries UUIDs via {@link BsonBinaries#uuidBinary} → subtype
 *       4 ({@code UUID_STANDARD}). BSON binary equality is
 *       subtype-aware: a v2 {@code findOne(_id = X-subtype4)} silently
 *       misses a stored subtype-0 doc.</li>
 *   <li><strong>{@code _id} vs shape's {@code idField}.</strong> Legacy
 *       stores the @Id value ONLY in Mongo's {@code _id}. v2 codec
 *       expects it under the shape's {@code idField()} (e.g.
 *       {@code session_id}). The doc-read path's
 *       {@link MongoEntityAdapter#projectIdIfMissing} aliases at read
 *       time too, but pre-aliasing here lets index queries on the
 *       idField column work too.</li>
 * </ol>
 *
 * <p><strong>Staging + rename pattern.</strong> Naively rewriting in
 * place would mean delete-then-insert per row (because {@code _id} is
 * immutable on update), which loses data if the process dies between
 * the two ops. Instead this migration uses the same staging + rename
 * pattern as the violations timeseries migration — except entity
 * collections are regular collections so the rename works without
 * the view restriction:
 *
 * <ol>
 *   <li>If canonical doesn't exist OR is empty → nothing to do.</li>
 *   <li>If canonical's first doc already has subtype-4 _id and
 *       idField populated → already migrated, skip.</li>
 *   <li>Otherwise:
 *     <ol type="a">
 *       <li>Drop any leftover staging collection from a prior failure.</li>
 *       <li>Stream-copy from canonical into staging, rewriting UUID
 *           subtypes + projecting _id → idField on each doc.</li>
 *       <li>Verify {@code staging.count == canonical.count}.</li>
 *       <li>Atomic two-rename: {@code canonical → _v6_bak},
 *           {@code staging → canonical} (both allowed because both
 *           are regular collections, unlike the violations timeseries
 *           case where staging is a view).</li>
 *     </ol>
 *   </li>
 * </ol>
 *
 * <p>Crash safety: between (a) and (d), the canonical collection is
 * untouched. Two recovery branches handle the partial-swap state:
 * <ul>
 *   <li><strong>Strict (1a):</strong> canonical absent + backup
 *       present → second rename never ran (or staging itself was lost
 *       to a crash inside a prior recovery rebuild). Staging is
 *       OPTIONAL/disposable here.</li>
 *   <li><strong>Loose (1b):</strong> canonical present but empty +
 *       backup present → same case as (1a), but
 *       {@code adapter.ensureStore} ran between the crash and the
 *       rerun and recreated canonical empty.</li>
 * </ul>
 * Both branches rebuild staging from backup via the idempotent
 * {@link #rebuildStaging} stream-copy (drop existing → create →
 * apply indexes → stream-copy → count-verify). The rebuild eliminates
 * the count-only-proof gap where an unrelated staging could be
 * falsely promoted. After rebuild, a {@link #dropIfEmptyOrThrow}
 * recheck on canonical defends against a concurrent
 * {@code ensureStore} or writer that may have appeared during the
 * rebuild window, then staging → canonical.
 *
 * <p>Main-path guard:
 * <ul>
 *   <li>Canonical non-empty + backup exists at fail-fast time → throw
 *       (the backup could be an orphan from a prior successful
 *       migration whose canonical the operator later recreated as v6,
 *       so silently dropping it would lose data the rename can't
 *       replace). Fail-fast occurs BEFORE the expensive rebuild.</li>
 * </ul>
 * A leftover staging at the main-path stage (after backup-existence
 * has been verified absent) can only be from a prior crashed main-
 * path attempt; {@link #rebuildStaging} drops + recreates it as part
 * of normal scratch handling.
 *
 * <p>Applied to every UUID-keyed Entity ({@code sessions},
 * {@code players}, etc.). String-keyed entities ({@code checks}) short-
 * circuit at the no-UUID-fields check. Empty collections short-circuit
 * at the precondition check.
 */
@ApiStatus.Internal
public final class MongoEntityV6ToV7UuidSubtypeMigration implements Migration<Entity<?, ?, ?>> {

    private static final String STAGING_SUFFIX = "_v7_migrating";
    private static final String BACKUP_SUFFIX  = "_v6_bak";

    @Override public int fromVersion() { return 0; }
    @Override public int toVersion()   { return 1; }

    @Override
    public void apply(@NotNull MigrationContext ctx, @NotNull StoreId id, @NotNull Entity<?, ?, ?> kind) throws Exception {
        if (!(ctx instanceof MongoMigrationContext mctx)) {
            throw new IllegalArgumentException(
                "MongoEntityV6ToV7UuidSubtypeMigration requires a MongoMigrationContext; got " + ctx.getClass().getName());
        }
        EncodeShape shape = kind.codec().shape();
        List<String> uuidFields = new ArrayList<>();
        boolean idIsUuid = false;
        for (EncodeShape.FieldDef f : shape.fields()) {
            if (f.javaType() == UUID.class) {
                uuidFields.add(f.name());
                if (f.name().equals(shape.idField())) idIsUuid = true;
            }
        }
        if (uuidFields.isEmpty()) {
            // No UUID fields → impedance doesn't apply. E.g. checks
            // (stableKey is String) — nothing to do.
            return;
        }
        run(mctx.database(), mctx.logger(), id, kind, uuidFields, idIsUuid, shape.idField());
    }

    private void run(@NotNull MongoDatabase db, @NotNull Logger logger, @NotNull StoreId id,
                     @NotNull Entity<?, ?, ?> kind,
                     @NotNull List<String> uuidFields, boolean idIsUuid, @NotNull String idField) {
        String canonical = id.name();
        String staging   = canonical + STAGING_SUFFIX;
        String backup    = canonical + BACKUP_SUFFIX;

        // (1a) Strict partial-swap recovery: canonical absent + backup
        // present. Staging is OPTIONAL/DISPOSABLE — we rebuild it from
        // backup via the idempotent stream-copy regardless. (Earlier
        // designs required staging to exist, but the recovery rebuild
        // itself drops and recreates staging, so requiring it created
        // an irrecoverable gap if the JVM died between drop and create.)
        if (!collectionExists(db, canonical) && collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            logger.warning(() -> "recovering: canonical missing, " + backup + " (" + backupCount
                + ") present — rebuilding staging from backup, completing second rename");
            long copied = rebuildStaging(db, backup, staging, kind, uuidFields, idIsUuid, idField, logger);
            verifyRecoveryRebuild(canonical, backup, staging, copied, backupCount);
            // If ensureStore (or anything else) recreated canonical
            // during the rebuild, drop it if empty / throw if not.
            dropIfEmptyOrThrow(db, canonical);
            db.getCollection(staging).renameCollection(
                new MongoNamespace(db.getName(), canonical), new RenameCollectionOptions());
            return;
        }

        // (1b) Loose partial-swap recovery: canonical EMPTY + backup
        // present. Happens when adapter.ensureStore ran between the
        // crash and the rerun and recreated canonical as an empty
        // regular collection. Staging again OPTIONAL.
        if (collectionExists(db, canonical)
                && db.getCollection(canonical).countDocuments() == 0
                && collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            logger.warning(() -> "recovering: empty canonical, " + backup + " (" + backupCount
                + ") present (ensureStore likely ran between crash and rerun)"
                + " — rebuilding staging from backup, completing second rename");
            long copied = rebuildStaging(db, backup, staging, kind, uuidFields, idIsUuid, idField, logger);
            verifyRecoveryRebuild(canonical, backup, staging, copied, backupCount);
            // Canonical was verified empty above. Recheck and drop /
            // throw — symmetric to 1a, hardens against a concurrent
            // writer inserting between gate and rename.
            dropIfEmptyOrThrow(db, canonical);
            db.getCollection(staging).renameCollection(
                new MongoNamespace(db.getName(), canonical), new RenameCollectionOptions());
            return;
        }

        if (!collectionExists(db, canonical)) {
            logger.fine(() -> canonical + " does not exist; nothing to migrate");
            return;
        }
        MongoCollection<Document> src = db.getCollection(canonical);
        long sourceCount = src.countDocuments();
        if (sourceCount == 0) {
            logger.fine(() -> canonical + " is empty; nothing to migrate");
            return;
        }

        // (2) Already migrated? Probe the first doc for subtype-4 _id +
        // populated idField. If both look v2-shaped we assume the
        // migration ran already and skip. This is a heuristic — a
        // mixed-state collection (some rows migrated, some not) would
        // need a fuller pass. Phase 1 offline-only.
        if (looksAlreadyMigrated(src, idIsUuid, idField)) {
            if (collectionExists(db, staging)) {
                long stagingCount = db.getCollection(staging).countDocuments();
                logger.warning(() -> canonical + " is v7-shaped but " + staging + " ("
                    + stagingCount + ") still exists (stranded from prior successful migration);"
                    + " operator may drop it manually");
            }
            logger.fine(() -> canonical + " already looks v7-shaped (subtype-4 _id + " + idField
                + " present); migration skipped");
            return;
        }

        // Fail fast on existing backup BEFORE the expensive rebuild.
        // Canonical is now known v6-shaped + non-empty. If backup also
        // exists it could be an orphan from a prior successful migration
        // whose canonical the operator later recreated as v6 (re-import,
        // manual reset, dev cycle) — silently dropping it would lose
        // data the rename can't replace.
        if (collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            throw new IllegalStateException(
                "refusing to overwrite existing " + backup + " (rows=" + backupCount
                    + ") while migrating v6 canonical " + canonical + " (rows=" + sourceCount
                    + "); both contain v6-shaped data and cannot be safely reconciled."
                    + " Manual operator review required (decide which v6 source is"
                    + " authoritative, drop the other, then rerun migration).");
        }
        // After the fail-fast above we know backup is absent. Any
        // existing staging here can ONLY be from a prior crashed
        // main-path attempt (recovery branches above didn't match
        // because backup is absent). rebuildStaging drops + recreates,
        // so the leftover is implicitly handled, but log it for ops
        // visibility.
        if (collectionExists(db, staging)) {
            logger.warning(() -> "found leftover staging " + staging
                + " (prior crashed main-path attempt); will rebuild from canonical");
        }

        logger.info(() -> "migrating " + canonical + " v6 → v7 (UUID subtype + idField projection)");

        // (3a-c) Build staging from canonical.
        long copied = rebuildStaging(db, canonical, staging, kind, uuidFields, idIsUuid, idField, logger);
        long sourceCountFinal = src.countDocuments();
        if (sourceCountFinal != copied) {
            throw new IllegalStateException(
                "source/staging divergence: source=" + sourceCountFinal + " copied=" + copied
                    + " — re-run migration after stopping live writers (offline-only)");
        }

        // (3d) Two-rename atomic swap. Backup-existence already verified
        // absent at the fail-fast above; safe to rename.
        MongoNamespace canonicalNs = new MongoNamespace(db.getName(), canonical);
        MongoNamespace backupNs    = new MongoNamespace(db.getName(), backup);
        db.getCollection(canonical).renameCollection(backupNs, new RenameCollectionOptions());
        db.getCollection(staging).renameCollection(canonicalNs, new RenameCollectionOptions());

        long copiedFinal = copied;
        logger.info(() -> "migration complete: " + copiedFinal + " row(s) migrated; v6 collection preserved at " + backup);
    }

    /**
     * Drop any existing staging, recreate as a fresh regular collection,
     * apply the kind's secondary indexes, then stream-copy from
     * {@code sourceName} into staging while rewriting UUID subtypes and
     * projecting {@code _id} → {@code idField} per doc.
     *
     * <p>Used by both the main migration path (source = canonical) and
     * the recovery branches (source = backup). Returns the number of
     * docs copied. Caller is responsible for the subsequent rename(s).
     */
    private long rebuildStaging(@NotNull MongoDatabase db, @NotNull String sourceName,
                                @NotNull String staging, @NotNull Entity<?, ?, ?> kind,
                                @NotNull List<String> uuidFields, boolean idIsUuid,
                                @NotNull String idField, @NotNull Logger logger) {
        if (collectionExists(db, staging)) {
            logger.warning(() -> "found leftover staging " + staging + "; dropping");
            db.getCollection(staging).drop();
        }
        db.createCollection(staging);
        MongoCollection<Document> stagingColl = db.getCollection(staging);
        // Apply secondary indexes BEFORE the rename so they ride along
        // when staging becomes canonical.
        applySecondaryIndexes(stagingColl, kind, logger);

        MongoCollection<Document> source = db.getCollection(sourceName);
        long copied = 0;
        List<Document> batch = new ArrayList<>(500);
        for (Document d : source.find().noCursorTimeout(true).batchSize(500)) {
            batch.add(rewrite(d, uuidFields, idIsUuid, idField));
            if (batch.size() >= 500) {
                stagingColl.insertMany(batch);
                copied += batch.size();
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            stagingColl.insertMany(batch);
            copied += batch.size();
        }

        long stagingCount = stagingColl.countDocuments();
        if (stagingCount != copied) {
            throw new IllegalStateException(
                "staging post-copy count mismatch: copied=" + copied + " in_staging=" + stagingCount);
        }
        return copied;
    }

    /**
     * Drop the named collection if it exists and is empty; throw if it
     * has data. Used to clear a possibly-racy canonical recreate before
     * the final rename in recovery, while refusing to silently destroy
     * concurrent inserts. Drop of a missing collection is a no-op in
     * the Mongo driver, so callers don't need a separate check.
     *
     * <p><strong>Best-effort detection, not distributed coordination.</strong>
     * The {@code countDocuments() + drop()} sequence is not atomic — an
     * insert that lands between the count and the drop will be silently
     * lost. The migration's contract is offline-only; this helper
     * detects offline-contract violations on the local node and refuses
     * to act, but it cannot prevent a remote node's writes that race
     * past the count gate. Multi-server safety requires a migration
     * lock (future phase).
     */
    private static void dropIfEmptyOrThrow(@NotNull MongoDatabase db, @NotNull String name) {
        if (!collectionExists(db, name)) return;
        long count = db.getCollection(name).countDocuments();
        if (count > 0) {
            throw new IllegalStateException(
                "refusing to drop " + name + ": it has " + count + " row(s) (concurrent writer?)."
                    + " Manual operator review required.");
        }
        db.getCollection(name).drop();
    }

    /**
     * Verify the rebuild produced staging.count == backup.count. Because
     * recovery rebuilds staging from backup via the idempotent
     * stream-copy, the only reason this would fail is if backup itself
     * is mid-mutation or corrupted. Throw for operator review.
     */
    private static void verifyRecoveryRebuild(@NotNull String canonical, @NotNull String backup,
                                              @NotNull String staging, long copied, long backupCount) {
        if (copied != backupCount || (backupCount > 0 && copied <= 0)) {
            throw new IllegalStateException(
                "post-rebuild verification failed for " + canonical + ": " + staging + " has "
                    + copied + " docs but " + backup + " has " + backupCount
                    + " — copy did not reproduce backup. Manual operator review required.");
        }
    }

    private static @NotNull Document rewrite(@NotNull Document d, @NotNull List<String> uuidFields,
                                             boolean idIsUuid, @NotNull String idField) {
        Document out = new Document(d);
        for (String fieldName : uuidFields) {
            String storedName = fieldName.equals(idField) ? "_id" : fieldName;
            Object val = out.get(storedName);
            if (!(val instanceof Binary bin)) continue;
            if (bin.getType() != BsonBinarySubType.BINARY.getValue() || bin.getData().length != 16) continue;
            UUID u = bytesToUuid(bin.getData());
            out.put(storedName, BsonBinaries.uuidBinary(u));
        }
        // Project _id → idField (e.g. session_id) so the v2 codec
        // finds the field at the name it expects.
        if (idIsUuid && out.get(idField) == null) {
            Object curId = out.get("_id");
            if (curId != null) out.put(idField, curId);
        }
        return out;
    }

    private static boolean looksAlreadyMigrated(@NotNull MongoCollection<Document> coll,
                                                 boolean idIsUuid, @NotNull String idField) {
        Document first = coll.find().limit(1).first();
        if (first == null) return false;
        Object idVal = first.get("_id");
        if (idIsUuid) {
            // v2-shaped: _id is subtype-4 binary (driver decodes to UUID)
            // OR idField is populated.
            if (idVal instanceof UUID) return first.get(idField) != null;
            if (idVal instanceof Binary b
                    && b.getType() == BsonBinarySubType.UUID_STANDARD.getValue()) {
                return first.get(idField) != null;
            }
            return false;
        }
        // Non-UUID-keyed entity (shouldn't be in this migration's
        // scope, but defensive): just check the idField is populated.
        return first.get(idField) != null;
    }

    /**
     * Apply the kind's declared secondary indexes to a staging
     * collection so they survive the rename into canonical. Mirrors
     * the index-creation logic in
     * {@code MongoEntityAdapter.ensureStore} but operates on a
     * caller-supplied collection (staging) rather than deriving from
     * a {@link StoreId}. Swallows Mongo error 85 (IndexOptionsConflict)
     * so re-runs against a partially-indexed staging don't crash.
     */
    private static void applySecondaryIndexes(@NotNull MongoCollection<Document> coll,
                                              @NotNull Entity<?, ?, ?> kind,
                                              @NotNull Logger logger) {
        for (IndexSpec spec : kind.secondaryIndexes()) {
            tryCreateIndex(coll, buildIndex(spec),
                new IndexOptions().unique(spec.unique()).name(spec.name()), logger);
            if (spec.caseInsensitivePrefix()) {
                for (String f : spec.fields()) {
                    String field = stripDir(f);
                    tryCreateIndex(coll, Indexes.ascending(field + "_lower"),
                        new IndexOptions().name(spec.name() + "_lower"), logger);
                }
            }
        }
    }

    private static void tryCreateIndex(@NotNull MongoCollection<Document> coll, @NotNull Bson keys,
                                       @NotNull IndexOptions opts, @NotNull Logger logger) {
        try {
            coll.createIndex(keys, opts);
        } catch (com.mongodb.MongoCommandException mce) {
            if (mce.getErrorCode() == 85 /* IndexOptionsConflict */) {
                logger.fine(() -> "index " + opts.getName() + " on staging conflicts with existing equivalent; ignoring");
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

    private static boolean collectionExists(@NotNull MongoDatabase db, @NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    private static UUID bytesToUuid(byte[] b) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
        return new UUID(buf.getLong(), buf.getLong());
    }
}
