package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Override the on-disk name of a record component. Without this annotation
 * the codec lowercases the Java field name (snake_case via
 * {@code camelCaseToSnakeCase}); with it, the codec uses the literal value.
 * <p>
 * Use this whenever the stored name needs to differ from the Java
 * component name — for example, {@code occurredEpochMs} stored as
 * {@code occurred_at} (the latter being what Mongo timeseries
 * {@code timeField} expects and what the v6 schema uses).
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Name {
    String value();
}
