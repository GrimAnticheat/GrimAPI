package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as the event time. Required on the field named by
 * an {@code EventStream}'s {@code timestampField}; optional elsewhere.
 * <p>
 * Codec emits the field as a native time type when the target supports one
 * ({@code BSON Date}, {@code TIMESTAMPTZ}); raw {@code long} otherwise.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Timestamp {
}
