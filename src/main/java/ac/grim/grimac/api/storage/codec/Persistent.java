package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record type as persistent. The codec generator scans
 * {@code @Persistent} records for field-role annotations
 * ({@link Id}, {@link Partition}, {@link Timestamp}, {@link Indexed},
 * {@link Searchable}, {@link Value}) and produces a fast encoder/decoder
 * per target backend format.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Persistent {

    /** Codec version. Bump when the record shape changes incompatibly; see {@link Codec#version()}. */
    int version() default 1;
}
