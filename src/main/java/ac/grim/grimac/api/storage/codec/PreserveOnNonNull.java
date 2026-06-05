package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a persistent record component for "first non-null wins"
 * conflict semantics: when the row already exists with a non-null
 * value for this column, the incoming upsert leaves it unchanged;
 * when the existing value is null, the incoming value is stored.
 * Maps to {@link MergeMode#PRESERVE_ON_NON_NULL}.
 *
 * <p>Use for @Nullable reference-typed fields where the producer
 * might not always have a value to send but must not stomp on a
 * prior writer's value — e.g. {@code SessionRecord.instanceId} (a
 * prior owner shouldn't be overwritten by a heartbeat from a
 * producer that hadn't been wired up yet).
 *
 * <p>For primitive-long fields with a "sentinel" unset value
 * (e.g. {@code SessionRecord.closedAtEpochMs} with
 * {@code OPEN = 0L}) use {@link Sentinel @Sentinel} instead — the
 * primitive can't be null, so the {@code @PreserveOnNonNull}
 * comparison wouldn't have anything to check.
 *
 * <p>Adapter behaviour on conflict:
 * <ul>
 *   <li>SQL: {@code col = COALESCE(tbl.col, EXCLUDED.col)}</li>
 *   <li>Mongo: {@code col = $ifNull[$col, encoded.col]}</li>
 * </ul>
 *
 * <p>Mutually exclusive with the other merge annotations
 * ({@link InsertOnly}, {@link Sentinel}, {@link MergeMax},
 * {@link MergeMin}); the codec rejects a record that combines them
 * on the same component.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface PreserveOnNonNull {
}
