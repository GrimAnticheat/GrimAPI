package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as a partition / metadata key. For
 * {@code EventStream} Kinds these become the {@code partitionFields} the
 * backend exposes as queryable (Mongo timeseries {@code meta} sub-doc,
 * SQL partition column, Redis hash-tag); for {@code Entity} they participate
 * in secondary indexes named on the Kind builder; for {@code KeyValueScoped}
 * they form the scope tuple.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Partition {
}
