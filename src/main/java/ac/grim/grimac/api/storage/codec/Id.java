package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as the primary key. Exactly one component per
 * {@link Persistent} record may carry this annotation.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Id {
}
