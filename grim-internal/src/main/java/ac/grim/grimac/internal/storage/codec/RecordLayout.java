package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.EncodeShape.FieldDef;
import ac.grim.grimac.api.storage.codec.Persistent;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.RecordComponent;

/**
 * Obfuscation-safe record metadata the codec needs <em>beyond</em> the
 * {@link EncodeShape}: the per-persistent-field accessor name (to bind a
 * {@code MethodHandle} via {@code findVirtual} on the kept public accessor)
 * and the full canonical-constructor parameter types (to bind the constructor
 * via {@code findConstructor}).
 * <p>
 * Neither touches the {@code RecordComponents} class attribute, which Allatori
 * strips on processing (so {@code isRecord()} / {@code getRecordComponents()}
 * return false/null at runtime). Captured at build time by the codec-binding
 * tool for the builtin records ({@link CapturedBindings}); derived via
 * reflection ({@link #fromReflection}) for extension records and
 * un-obfuscated runs, where the attribute is intact.
 *
 * @param accessorNames  java accessor name per persistent field, in
 *                       {@link EncodeShape#fields()} order
 * @param ctorParamTypes full canonical-constructor parameter types, in
 *                       declaration order (persistent + transient)
 * @param version        {@link Persistent#version()} — captured here so the
 *                       codec never reads the class annotation at runtime
 */
@ApiStatus.Internal
public record RecordLayout(@NotNull String[] accessorNames, @NotNull Class<?>[] ctorParamTypes, int version) {

    /** Reflection path: read accessor names + ctor param types off the live record. */
    public static @NotNull RecordLayout fromReflection(@NotNull Class<?> recordType, @NotNull EncodeShape shape) {
        RecordComponent[] components = recordType.getRecordComponents();
        if (components == null) {
            throw new IllegalStateException(
                "no record components for " + recordType.getName()
                    + " — the RecordComponents attribute was stripped (obfuscation?) and no captured "
                    + "binding was found; ensure the record lives in the obf keep-set and was captured at build time");
        }
        Class<?>[] ctorParamTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) ctorParamTypes[i] = components[i].getType();

        var fields = shape.fields();
        String[] accessorNames = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            FieldDef f = fields.get(i);
            accessorNames[i] = components[f.recordIndex()].getName();
        }
        Persistent p = recordType.getAnnotation(Persistent.class);
        return new RecordLayout(accessorNames, ctorParamTypes, p == null ? 1 : p.version());
    }
}
