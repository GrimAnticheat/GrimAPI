package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a persistent record component for "keep the smaller value"
 * conflict semantics: on upsert against an existing row, the column
 * is set to {@code MIN(existing, incoming)}. Maps to
 * {@link MergeMode#MIN}.
 *
 * <p>Use for monotonic-decreasing fields where a late-arriving
 * later-observed value shouldn't widen the range — e.g.
 * {@code PlayerIdentity.firstSeenEpochMs}.
 *
 * <p>Adapter behaviour on conflict:
 * <ul>
 *   <li>SQL: {@code col = LEAST(tbl.col, EXCLUDED.col)} (or
 *       {@code MIN(...)} on dialects without LEAST).</li>
 *   <li>Mongo: {@code col = $min[$col, encoded.col]}</li>
 * </ul>
 *
 * <p>Mutually exclusive with the other merge annotations
 * ({@link InsertOnly}, {@link PreserveOnNonNull}, {@link MergeMax});
 * the codec rejects a record that combines them on the same component.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface MergeMin {
}
