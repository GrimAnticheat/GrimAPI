package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as a payload value: stored in the record body,
 * not indexed unless paired with {@link Indexed} or {@link Searchable}.
 * Default role for any record component without a more specific annotation.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Value {
}
