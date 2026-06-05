package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a persistent record component as INSERT-ONLY: the field is
 * written when the row is first inserted, but never updated on
 * subsequent upserts. Maps to {@link MergeMode#INSERT_ONLY}.
 * <p>
 * Use for surrogate keys that are referenced elsewhere by id —
 * e.g. {@code CheckCatalogRecord.checkId} (an integer id stamped on
 * first insert; if a republish allocated a new id under the same
 * {@code stableKey}, historical violation rows would be orphaned).
 *
 * <p>Adapter behaviour on conflict:
 * <ul>
 *   <li>SQL: column is OMITTED from the {@code DO UPDATE SET} clause.</li>
 *   <li>Mongo: the field is wrapped in a {@code $cond} that uses
 *       {@code $setOnInsert}-equivalent semantics inside the
 *       aggregation pipeline.</li>
 * </ul>
 *
 * <p>Mutually exclusive with the other merge annotations
 * ({@link PreserveOnNonNull}, {@link MergeMax}, {@link MergeMin}); the
 * codec rejects a record that combines them on the same component.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface InsertOnly {
}
