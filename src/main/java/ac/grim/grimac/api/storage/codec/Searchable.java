package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as searchable via the
 * {@link ac.grim.grimac.api.storage.search.SearchSpec} API. Requires the
 * backend to advertise a matching {@code SEARCH_*} capability for the
 * declared {@link SearchType}.
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Searchable {

    SearchType value();

    /** Dimension for {@link SearchType#VECTOR} fields. Ignored otherwise. */
    int dimension() default 0;
}
