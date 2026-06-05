package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a persistent record component for "keep the larger value"
 * conflict semantics: on upsert against an existing row, the column
 * is set to {@code MAX(existing, incoming)}. Maps to
 * {@link MergeMode#MAX}.
 *
 * <p>Use for monotonic-increasing fields where late-arriving updates
 * shouldn't shrink the value — e.g.
 * {@code PlayerIdentity.lastSeenEpochMs}.
 *
 * <p>Adapter behaviour on conflict:
 * <ul>
 *   <li>SQL: {@code col = GREATEST(tbl.col, EXCLUDED.col)} (or
 *       {@code MAX(...)} on dialects without GREATEST).</li>
 *   <li>Mongo: {@code col = $max[$col, encoded.col]}</li>
 * </ul>
 *
 * <p>Mutually exclusive with the other merge annotations
 * ({@link InsertOnly}, {@link PreserveOnNonNull}, {@link MergeMin});
 * the codec rejects a record that combines them on the same component.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface MergeMax {
}
