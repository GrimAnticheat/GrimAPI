package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as not persisted. The codec skips it entirely
 * on encode (the field never reaches the wire) and on decode (the
 * canonical constructor receives the type's zero/null default for that
 * argument).
 * <p>
 * Use for record components that live only in memory:
 * <ul>
 *   <li>Future-feature stubs whose serialisation isn't designed yet
 *       (e.g. {@code SessionRecord.sessionBlobs} before the Blob/SessionBlob
 *       Kind lands).</li>
 *   <li>Computed/cached fields that derive from other components.</li>
 *   <li>Anything whose Java type the codec can't encode (collections,
 *       arbitrary objects without their own codec).</li>
 * </ul>
 * Mutually exclusive with every other field-role annotation
 * ({@link Id}, {@link Partition}, {@link Timestamp}, {@link Indexed},
 * {@link Searchable}, {@link Value}). The codec rejects {@code @Transient}
 * combined with any of those at introspection time.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Transient {
}
