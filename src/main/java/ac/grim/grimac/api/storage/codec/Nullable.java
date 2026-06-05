package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Storage-owned nullability marker. JetBrains
 * {@code org.jetbrains.annotations.Nullable} is {@code CLASS} retention, so it
 * cannot be reflected at runtime — using it for storage decisions silently
 * fails. This annotation is {@code RUNTIME} retention and is the only signal
 * the codec layer reads for "this field may be missing on disk."
 * <p>
 * Encoding rules:
 * <ul>
 *   <li>Reference field with {@code @Nullable}: a null value is omitted from
 *       the encoded form. Decode tolerates a missing field.</li>
 *   <li>Reference field without {@code @Nullable}: a null value at encode
 *       throws; a missing field at decode throws.</li>
 *   <li>Primitive fields cannot carry {@code @Nullable} — codec rejects at
 *       introspection time.</li>
 * </ul>
 */
@ApiStatus.Experimental
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface Nullable {
}
