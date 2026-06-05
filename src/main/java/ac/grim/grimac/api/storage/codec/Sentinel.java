package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a primitive-long record component for "first non-sentinel
 * value wins" conflict semantics. Lets the merge-pipeline machinery
 * apply preserve-style semantics to a field whose Java type is a
 * non-nullable primitive — the sentinel value substitutes for null
 * in the existing-vs-incoming comparison.
 *
 * <p>Use for primitive longs that carry an "unset" sentinel:
 * {@code SessionRecord.closedAtEpochMs} with {@code OPEN = 0L} is
 * the canonical case. A heartbeat upsert sets {@code closedAtEpochMs = 0},
 * but a previous close event's real timestamp must survive — the
 * sentinel-aware preserve sees existing != 0 and keeps it.
 *
 * <p>Maps to {@link MergeMode#PRESERVE_ON_NON_SENTINEL}.
 *
 * <p>Adapter behaviour on conflict:
 * <ul>
 *   <li>SQL: {@code col = CASE WHEN tbl.col IS NULL OR tbl.col = <sentinel>
 *       THEN EXCLUDED.col ELSE tbl.col END}</li>
 *   <li>Mongo (aggregation pipeline):
 *       {@code col = $cond[$or[$in[$type[$col], ["missing","null"]],
 *       $eq[$col, <sentinel>]], encoded.col, $col]}.
 *       The {@code $in} branch covers both absent fields AND
 *       explicit BSON null (legacy MongoBackend writes open
 *       sessions as {@code closed_at:null}); the {@code $eq} branch
 *       covers the primitive-long sentinel from a v2 codec write.</li>
 * </ul>
 *
 * <p>Validation:
 * <ul>
 *   <li>Target type MUST be primitive {@code long}. Reference types
 *       use {@link PreserveOnNonNull} (null IS the sentinel for them);
 *       other primitives don't have a documented use case yet.</li>
 *   <li>Mutually exclusive with the other merge annotations
 *       ({@link InsertOnly}, {@link PreserveOnNonNull}, {@link MergeMax},
 *       {@link MergeMin}).</li>
 *   <li>Forbidden on {@link Id @Id} (primary keys are already
 *       insert-once by definition).</li>
 * </ul>
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Sentinel {
    /** Sentinel value that substitutes for null in the preserve check. */
    long value() default 0L;
}
