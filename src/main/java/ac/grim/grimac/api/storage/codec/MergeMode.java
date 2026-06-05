package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

/**
 * Per-field merge policy applied by an adapter on conflicting upsert
 * (the row already exists). Drives how the adapter compiles the
 * {@code ON CONFLICT DO UPDATE SET} clause (SQL) or the aggregation
 * pipeline (Mongo). Defaults to {@link #OVERWRITE} when no merge
 * annotation is present.
 *
 * <p>The mode is derived from field annotations:
 * <ul>
 *   <li>{@link InsertOnly @InsertOnly} → {@link #INSERT_ONLY}</li>
 *   <li>{@link PreserveOnNonNull @PreserveOnNonNull} → {@link #PRESERVE_ON_NON_NULL}</li>
 *   <li>{@link Sentinel @Sentinel} → {@link #PRESERVE_ON_NON_SENTINEL}</li>
 *   <li>{@link MergeMax @MergeMax} → {@link #MAX}</li>
 *   <li>{@link MergeMin @MergeMin} → {@link #MIN}</li>
 *   <li>none → {@link #OVERWRITE}</li>
 * </ul>
 *
 * <p>Adapter compilation per mode:
 * <pre>
 *   Mode                       SQL (ON CONFLICT DO UPDATE SET …)                                   Mongo (aggregation pipeline $set)
 *   =========================  ==================================================================  ====================================================================
 *   OVERWRITE                  col = EXCLUDED.col                                                  {col: encoded.col}
 *   INSERT_ONLY                — (omit from SET; EXCLUDED still lands on first INSERT)              {col: {$cond: [{$eq: [{$type: $col}, "missing"]}, encoded.col, $col]}}
 *   PRESERVE_ON_NON_NULL       col = COALESCE(tbl.col, EXCLUDED.col)                               {col: {$ifNull: [$col, encoded.col]}}
 *   PRESERVE_ON_NON_SENTINEL   col = CASE WHEN tbl.col IS NULL OR tbl.col = <sentinel>             {col: {$cond: [{$or: [{$in: [{$type: $col}, ["missing","null"]]},
 *                                           THEN EXCLUDED.col ELSE tbl.col END                                          {$eq: [$col, <sentinel>]}]}, encoded.col, $col]}}
 *   MAX                        col = GREATEST(tbl.col, EXCLUDED.col)                               {col: {$max: [$col, encoded.col]}}
 *   MIN                        col = LEAST(tbl.col, EXCLUDED.col)                                  {col: {$min: [$col, encoded.col]}}
 * </pre>
 *
 * <p>Examples on built-in records:
 * <ul>
 *   <li>{@code SessionRecord.closedAtEpochMs} — {@link #PRESERVE_ON_NON_SENTINEL}
 *       with sentinel {@code 0L} ({@code SessionRecord.OPEN}). Heartbeats
 *       send the sentinel; a real close timestamp survives subsequent
 *       heartbeats.</li>
 *   <li>{@code SessionRecord.instanceId} — {@link #PRESERVE_ON_NON_NULL}
 *       so a prior non-null owner survives heartbeats from producers
 *       that aren't wired yet.</li>
 *   <li>{@code PlayerIdentity.firstSeenEpochMs} — {@link #MIN}.</li>
 *   <li>{@code PlayerIdentity.lastSeenEpochMs} — {@link #MAX}.</li>
 *   <li>{@code CheckCatalogRecord.checkId} — {@link #INSERT_ONLY} so
 *       republishing a stable_key can't orphan historical violation
 *       rows that reference the old check_id.</li>
 *   <li>{@code CheckCatalogRecord.introducedAt} — {@link #MIN} so the
 *       earliest observed registration time wins across the fleet.</li>
 * </ul>
 */
@ApiStatus.Experimental
public enum MergeMode {
    OVERWRITE,
    INSERT_ONLY,
    PRESERVE_ON_NON_NULL,
    /**
     * Like {@link #PRESERVE_ON_NON_NULL} but for primitive-long fields
     * carrying an "unset" {@link Sentinel} value. The sentinel value
     * (typically {@code 0L}) substitutes for null in the
     * existing-vs-incoming comparison so a heartbeat that sends the
     * sentinel can't clobber a real prior write. The sentinel value
     * is captured at codec introspection time on
     * {@link EncodeShape.FieldDef#sentinelValue()}.
     */
    PRESERVE_ON_NON_SENTINEL,
    MAX,
    MIN
}
