package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.kind.KeyValueScoped;
import ac.grim.grimac.api.storage.registry.Migration;
import ac.grim.grimac.api.storage.registry.MigrationContext;
import ac.grim.grimac.api.storage.registry.StoreId;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.RenameCollectionOptions;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Migrate {@code grim_settings} from the v6 layout (one document per
 * {@code (scope, scope_key, key)} with a flat {@code value} field) to the
 * v7 layout (one document per {@code (scope, scope_key)} with a
 * {@code values} sub-document and embedded keys).
 *
 * <p>Algorithm — server-side aggregation; scales arbitrarily:
 * <ol>
 *   <li>Short-circuit if the canonical collection is already v7-shaped
 *       (detected by a top-level {@code values} sub-document on any sample
 *       doc; legacy v6 docs have a flat {@code value} field instead).</li>
 *   <li>Drop any leftover staging collection from a prior failed run.</li>
 *   <li>Recover from a partial 2-rename: if canonical is missing AND backup
 *       exists (strict), or canonical exists but is empty AND backup exists
 *       (loose — the {@code ensureStore}-recreated-canonical case), DROP any
 *       existing staging and REBUILD it from backup via the same aggregation
 *       (idempotent). Staging is OPTIONAL/disposable — earlier designs that
 *       required staging to exist created an irrecoverable gap if the JVM
 *       died between staging drop and staging recreate. Then verify the
 *       rebuilt staging's tenant count matches backup's distinct tenants,
 *       drop-or-throw on any racily-recreated canonical, and complete the
 *       second rename.</li>
 *   <li>Run a single Mongo aggregation:
 *       <pre>
 *       [
 *         { $group: {
 *             _id: { scope: "$scope", scope_key: "$scope_key" },
 *             pairs: { $push: { k: "$key", v: "$value" } },
 *             updated_at: { $max: "$updated_at" }
 *         } },
 *         { $project: {
 *             values: { $arrayToObject: "$pairs" },
 *             updated_at: 1
 *         } },
 *         { $out: "&lt;staging&gt;" }
 *       ]
 *       </pre>
 *       which atomically writes the per-tenant envelopes to the staging
 *       collection.</li>
 *   <li>No companion unique index is created on staging — uniqueness on
 *       the composite {@code (scope, scope_key)} key is already enforced
 *       by Mongo's automatic unique {@code _id} index, since the
 *       aggregation writes the composite as a single {@code _id} subdoc.</li>
 *   <li>Verify the destination tenant count matches a separate
 *       {@code $group} count on the source.</li>
 *   <li>Atomic 2-rename swap: canonical → {@code _v6_bak},
 *       staging → canonical. Refuse and throw if an existing backup is
 *       found at swap time — it could be an orphan from a prior successful
 *       migration whose canonical the operator later recreated as v6, and
 *       silently destroying it would lose data the rename can't replace.</li>
 * </ol>
 *
 * <p>Idempotent on re-run; resumable on partial-rename failure.
 */
@ApiStatus.Internal
public final class MongoSettingsV6ToV7Migration implements Migration<KeyValueScoped<?, ?>> {

    private static final String STAGING_SUFFIX = "_v7_migrating";
    private static final String BACKUP_SUFFIX  = "_v6_bak";

    @Override public int fromVersion() { return 0; }   // pre-v7 baseline
    @Override public int toVersion()   { return 1; }   // v7-shaped per-tenant envelope

    @Override
    public void apply(@NotNull MigrationContext ctx, @NotNull StoreId id, @NotNull KeyValueScoped<?, ?> kind) throws Exception {
        if (!(ctx instanceof MongoMigrationContext mctx)) {
            throw new IllegalArgumentException(
                "MongoSettingsV6ToV7Migration requires a MongoMigrationContext; got " + ctx.getClass().getName());
        }
        run(mctx.database(), mctx.logger(), id);
    }

    private void run(@NotNull MongoDatabase db, @NotNull Logger logger, @NotNull StoreId id) throws Exception {
        String canonical = id.name();
        String staging   = canonical + STAGING_SUFFIX;
        String backup    = canonical + BACKUP_SUFFIX;

        // 1a. Strict partial-swap recovery: canonical absent + backup
        // present. Staging is OPTIONAL/DISPOSABLE — we rebuild it from
        // backup via the idempotent aggregation regardless. (Earlier
        // designs required staging to exist, but the recovery rebuild
        // itself drops and recreates staging, so requiring it created
        // an irrecoverable gap if the JVM died between drop and create.)
        if (!collectionExists(db, canonical) && collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            logger.warning(() -> "recovering: canonical missing, " + backup + " (rows=" + backupCount
                + ") present — rebuilding staging from backup, completing second rename");
            if (collectionExists(db, staging)) db.getCollection(staging).drop();
            aggregateInto(db, backup, staging);
            verifyStagingMatchesBackup(db, canonical, staging, backup, logger);
            // If ensureStore (or anything else) recreated canonical
            // during the rebuild, drop it if empty / throw if not, so
            // the rename has a clear target.
            dropIfEmptyOrThrow(db, canonical);
            db.getCollection(staging).renameCollection(
                new MongoNamespace(db.getName(), canonical), new RenameCollectionOptions());
            return;
        }
        // 1b. Loose partial-swap recovery: canonical EMPTY + backup
        // present. Happens when adapter.ensureStore ran between the
        // crash and the rerun and recreated canonical as an empty
        // regular collection. Same shape as 1a, plus we drop the empty
        // canonical at the end. Staging is again OPTIONAL.
        if (collectionExists(db, canonical)
                && db.getCollection(canonical).countDocuments() == 0
                && collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            logger.warning(() -> "recovering: empty canonical, " + backup + " (rows=" + backupCount
                + ") present (ensureStore likely ran between crash and rerun)"
                + " — rebuilding staging from backup, completing second rename");
            if (collectionExists(db, staging)) db.getCollection(staging).drop();
            aggregateInto(db, backup, staging);
            verifyStagingMatchesBackup(db, canonical, staging, backup, logger);
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
        if (isAlreadyV7Shaped(db, canonical)) {
            if (collectionExists(db, staging)) {
                long stagingCount = db.getCollection(staging).countDocuments();
                logger.warning(() -> canonical + " is v7-shaped but " + staging + " ("
                    + stagingCount + ") still exists (stranded from prior successful migration);"
                    + " operator may drop it manually");
            }
            logger.fine(() -> canonical + " is already v7-shaped (envelope docs); migration skipped");
            return;
        }
        // Fail fast on existing backup BEFORE the expensive aggregation.
        // Canonical is now known v6-shaped + non-empty. If backup also
        // exists it could be an orphan from a prior successful migration
        // whose canonical the operator later recreated as v6 (re-import,
        // manual reset, dev cycle) — silently dropping it would lose
        // data the rename can't replace.
        if (collectionExists(db, backup)) {
            long backupCount = db.getCollection(backup).countDocuments();
            long canonicalCount = db.getCollection(canonical).countDocuments();
            throw new IllegalStateException(
                "refusing to overwrite existing " + backup + " (rows=" + backupCount
                    + ") while migrating v6 canonical " + canonical + " (rows=" + canonicalCount
                    + "); both contain v6-shaped data and cannot be safely reconciled."
                    + " Manual operator review required (decide which v6 source is"
                    + " authoritative, drop the other, then rerun migration).");
        }
        // After the fail-fast above we know backup is absent. Any
        // existing staging here can ONLY be from a prior crashed
        // main-path attempt (recovery branches above didn't match
        // because backup is absent). Safe to drop and re-aggregate.
        if (collectionExists(db, staging)) {
            logger.warning(() -> "found leftover staging " + staging
                + " (prior crashed main-path attempt); dropping");
            db.getCollection(staging).drop();
        }
        logger.info(() -> "migrating " + canonical + " from v6 -> v7 (per-tenant envelopes)");

        MongoCollection<Document> src = db.getCollection(canonical);
        long sourceCount = src.countDocuments();

        // 2. Server-side aggregation: group → arrayToObject → $out.
        aggregateInto(db, canonical, staging);

        // 3. No additional index on staging. Mongo's automatic unique _id
        // index already enforces uniqueness on the composite (scope,
        // scope_key) — adding a compound (_id.scope, _id.scope_key) would
        // be dead weight for the equality-only access pattern (matches
        // MongoKeyValueScopedAdapter.ensureStore which dropped its compound
        // in Phase 2.3 for the same reason).

        // 4. Verify count: staging tenant count == distinct (scope, scope_key) in source.
        long sourceTenants = countDistinctTenants(src);
        long stagingCount = db.getCollection(staging).countDocuments();
        if (stagingCount != sourceTenants) {
            throw new IllegalStateException(
                "tenant count mismatch: source distinct=" + sourceTenants
                    + " staging docs=" + stagingCount + " (source rows=" + sourceCount + ")");
        }

        // 5. Atomic 2-rename swap. Recovery branches above handle a JVM
        // death between the two renames. Backup-existence already
        // verified absent at the fail-fast above; safe to rename.
        MongoNamespace canonicalNs = new MongoNamespace(db.getName(), canonical);
        MongoNamespace backupNs    = new MongoNamespace(db.getName(), backup);
        db.getCollection(canonical).renameCollection(backupNs, new RenameCollectionOptions());
        db.getCollection(staging).renameCollection(canonicalNs, new RenameCollectionOptions());

        long finalCount = stagingCount;
        logger.info(() -> "settings migration complete: " + sourceCount + " v6 rows -> "
            + finalCount + " v7 tenant envelopes; v6 collection preserved at " + backup);
    }

    /**
     * Server-side aggregation: group v6 per-setting rows into per-tenant
     * envelopes and {@code $out} to the named target. Used by both the
     * main migration path and the recovery branches (which rebuild
     * staging rather than trust an existing one). The {@code $out}
     * operator atomically replaces the target — wrap in try/catch so
     * partial failure cleans up immediately rather than leaving orphan
     * staging.
     */
    private static void aggregateInto(@NotNull MongoDatabase db, @NotNull String sourceName,
                                      @NotNull String target) {
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document()
                .append("_id", new Document()
                    .append("scope", "$scope")
                    .append("scope_key", "$scope_key"))
                .append("pairs", new Document("$push", new Document()
                    .append("k", "$key")
                    .append("v", "$value")))
                .append("updated_at", new Document("$max", "$updated_at"))),
            new Document("$project", new Document()
                .append("values", new Document("$arrayToObject", "$pairs"))
                .append("updated_at", 1)),
            new Document("$out", target));
        try {
            // toCollection triggers $out execution (find().forEach would not).
            db.getCollection(sourceName).aggregate(pipeline).toCollection();
        } catch (RuntimeException e) {
            // Best-effort cleanup of half-written target; rethrow.
            try {
                db.getCollection(target).drop();
            } catch (RuntimeException ignored) { /* secondary failure */ }
            throw e;
        }
    }

    // ============================== helpers ==============================

    private static boolean collectionExists(@NotNull MongoDatabase db, @NotNull String name) {
        for (String existing : db.listCollectionNames()) {
            if (existing.equals(name)) return true;
        }
        return false;
    }

    /**
     * Heuristic detection of "already migrated": a v6 doc has a top-level
     * {@code key} field (per-setting layout); a v7 doc has a top-level
     * {@code values} field (per-tenant envelope) instead. Sample one doc
     * to decide. Empty collections are treated as "not v7" — the migration
     * will then no-op via the aggregation against an empty source.
     */
    private static boolean isAlreadyV7Shaped(@NotNull MongoDatabase db, @NotNull String name) {
        Document sample = db.getCollection(name).find().limit(1).first();
        if (sample == null) return false;
        return sample.containsKey("values") && !sample.containsKey("key");
    }

    /**
     * Drop the named collection if it exists and is empty; throw if it
     * has data. Used to clear a possibly-racy canonical recreate before
     * the final rename, while refusing to silently destroy concurrent
     * inserts. Drop of a missing collection is a no-op in the Mongo
     * driver, so callers don't need a separate existence check.
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
     * Verify the post-recovery staging count matches the backup's
     * distinct tenant count. Because the recovery path REBUILDS
     * staging from backup via the idempotent aggregation, this is a
     * sanity check that the rebuild didn't silently mis-count (e.g.
     * because of a partial backup or aggregation error). A mismatch
     * means the backup is corrupted or the aggregation didn't behave
     * as expected; throw rather than promote.
     */
    private static void verifyStagingMatchesBackup(@NotNull MongoDatabase db, @NotNull String canonical,
                                                   @NotNull String staging, @NotNull String backup,
                                                   @NotNull Logger logger) {
        long stagingCount = db.getCollection(staging).countDocuments();
        long backupCount  = db.getCollection(backup).countDocuments();
        long backupTenants = countDistinctTenants(db.getCollection(backup));
        if (stagingCount != backupTenants || (backupCount > 0 && stagingCount <= 0)) {
            throw new IllegalStateException(
                "post-rebuild verification failed for " + canonical + ": " + staging + " has "
                    + stagingCount + " tenant envelopes but " + backup + " has " + backupCount
                    + " rows / " + backupTenants + " distinct tenants; aggregation did not"
                    + " produce expected result. Manual operator review required.");
        }
        logger.fine(() -> "post-rebuild verified: " + staging + "=" + stagingCount
            + " tenants matches " + backup + "_tenants=" + backupTenants);
    }

    /** Count distinct (scope, scope_key) tuples in the source — the expected destination count. */
    private static long countDistinctTenants(@NotNull MongoCollection<Document> src) {
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document("_id", new Document()
                .append("scope", "$scope")
                .append("scope_key", "$scope_key"))),
            new Document("$count", "n"));
        Document r = src.aggregate(pipeline).first();
        return r == null ? 0L : ((Number) r.get("n")).longValue();
    }
}
