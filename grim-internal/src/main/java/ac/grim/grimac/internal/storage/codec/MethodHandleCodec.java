package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.Codec;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.EncodeShape.FieldDef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Shipping codec implementation. One instance per {@code @Persistent} record
 * class; parallel arrays of (name, type-tag, typed-accessor) drive the
 * per-format encode / decode dispatch loops in subclasses
 * (BsonCodecImpl / JdbcCodecImpl / RedisCodecImpl in their respective
 * subpackages).
 * <p>
 * Built at startup via {@link MethodHandleCodecFactory}; cached forever.
 * Hot encode: ~120 ns for the violations shape. No bytecode generation, no
 * runtime reflection in the loop.
 */
@ApiStatus.Internal
public class MethodHandleCodec<R> implements Codec<R> {

    private final @NotNull Class<R> recordType;
    private final @NotNull EncodeShape shape;
    private final int version;

    // Parallel arrays — same length as shape.fields(). Cache-friendly iteration.
    protected final @NotNull String[] fieldNames;
    protected final @NotNull TypeTag[] typeTags;
    /** Mix of ToIntFunction / ToLongFunction / ToDoubleFunction / Function. */
    protected final @NotNull Object[] accessors;
    protected final boolean[] nullable;
    protected final @NotNull Class<?>[] javaTypes;
    /**
     * For ENUM fields, the constants array cached once at codec init.
     * Decode looks up by ordinal in O(1) without a per-call reflective
     * {@code enumType.getMethod("values")}.
     */
    protected final Enum<?> @NotNull [][] enumConstants;

    /** Maps encoded field name -> index into the parallel arrays. */
    private final @NotNull Map<String, Integer> fieldIndex;

    /**
     * Per-persistent-field position in the record's FULL component list.
     * Decode places each decoded value at this position in the canonical
     * constructor's argument array; transient positions are left at the
     * Java type default.
     */
    private final int[] recordPositions;

    /** Total component count of the record (persistent + transient). */
    private final int totalComponents;

    /** Canonical constructor of the record, cached for decode. */
    protected final @NotNull MethodHandle constructor;

    public MethodHandleCodec(@NotNull Class<R> recordType, @NotNull EncodeShape shape) {
        this.recordType = recordType;
        this.shape = shape;

        // Obfuscation-safe layout: prefer the build-time capture, because Allatori
        // strips the RecordComponents attribute so getRecordComponents()/isRecord()
        // fail at runtime. Fall back to live reflection for extension records and
        // un-obfuscated runs, where the attribute is intact. Accessor names and the
        // ctor param types are then bound by name+type against the kept api surface.
        RecordLayout layout = CapturedBindings.layout(recordType);
        if (layout == null) layout = RecordLayout.fromReflection(recordType, shape);
        this.version = layout.version();

        List<FieldDef> defs = shape.fields();
        int n = defs.size();
        this.fieldNames = new String[n];
        this.typeTags = new TypeTag[n];
        this.accessors = new Object[n];
        this.nullable = new boolean[n];
        this.javaTypes = new Class<?>[n];
        this.enumConstants = new Enum<?>[n][];
        this.recordPositions = new int[n];
        this.fieldIndex = new HashMap<>(n * 2);

        this.totalComponents = shape.totalComponentCount();
        String[] accessorNames = layout.accessorNames();
        for (int i = 0; i < n; i++) {
            FieldDef def = defs.get(i);
            fieldNames[i] = def.name();
            typeTags[i] = TypeTag.of(def.javaType());
            // accessorNames is parallel to shape.fields(); each name is the kept
            // public record accessor — Accessors.build binds it via findVirtual.
            accessors[i] = Accessors.build(recordType, accessorNames[i], def.javaType());
            nullable[i] = def.nullable();
            javaTypes[i] = def.javaType();
            recordPositions[i] = def.recordIndex();
            if (typeTags[i] == TypeTag.ENUM) {
                @SuppressWarnings("unchecked")
                Class<? extends Enum<?>> enumType = (Class<? extends Enum<?>>) def.javaType();
                enumConstants[i] = enumType.getEnumConstants();
            }
            fieldIndex.put(def.name(), i);
        }

        try {
            this.constructor = canonicalConstructorHandle(recordType, layout.ctorParamTypes());
        } catch (Throwable t) {
            throw new IllegalStateException(
                "could not resolve canonical constructor for " + recordType.getName(), t);
        }
    }

    /** Position of persistent field {@code i} in the record's full component list. */
    public int recordPositionAt(int i) { return recordPositions[i]; }

    /** Total component count of the record (persistent + transient). */
    public int totalComponentCount() { return totalComponents; }

    @Override public @NotNull Class<R> recordType() { return recordType; }
    @Override public @NotNull EncodeShape shape()   { return shape; }
    @Override public int version()                  { return version; }

    @Override
    public @Nullable Object indexField(@NotNull R record, @NotNull String fieldName) {
        Integer idx = fieldIndex.get(fieldName);
        if (idx == null) {
            throw new IllegalArgumentException(
                "no such field on " + recordType.getName() + ": " + fieldName);
        }
        return readField(record, idx);
    }

    // ---- Helpers used by per-format subclasses ----

    /** Read a single field as a boxed Object. Used by indexField + decode reconstruction. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected @Nullable Object readField(@NotNull R record, int i) {
        Object acc = accessors[i];
        return switch (typeTags[i]) {
            case INT         -> ((ToIntFunction) acc).applyAsInt(record);
            case LONG        -> ((ToLongFunction) acc).applyAsLong(record);
            case DOUBLE,
                 FLOAT       -> ((ToDoubleFunction) acc).applyAsDouble(record);
            case ENUM,
                 BOOLEAN,
                 STRING,
                 BYTES,
                 UUID,
                 FLOAT_ARRAY,
                 NESTED_SEALED -> ((Function) acc).apply(record);
        };
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected int readInt(@NotNull R record, int i)       { return ((ToIntFunction) accessors[i]).applyAsInt(record); }
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected long readLong(@NotNull R record, int i)     { return ((ToLongFunction) accessors[i]).applyAsLong(record); }
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected double readDouble(@NotNull R record, int i) { return ((ToDoubleFunction) accessors[i]).applyAsDouble(record); }
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected @Nullable Object readRef(@NotNull R record, int i) { return ((Function) accessors[i]).apply(record); }

    /** Read an enum field's ordinal — accessor is a Function<R, Enum<?>>, value cannot be null. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected int readEnumOrdinal(@NotNull R record, int i) {
        Enum<?> e = (Enum<?>) ((Function) accessors[i]).apply(record);
        if (e == null) {
            throw new IllegalStateException(
                "null enum value for non-nullable component " + fieldNames[i] + " on " + recordType.getName());
        }
        return e.ordinal();
    }

    /** Resolve an enum constant by ordinal using the cached constants array. */
    public @NotNull Enum<?> enumFromOrdinal(int i, int ordinal) {
        Enum<?>[] consts = enumConstants[i];
        if (consts == null) {
            throw new IllegalStateException(
                "enumFromOrdinal called for non-enum field " + fieldNames[i] + " on " + recordType.getName());
        }
        if (ordinal < 0 || ordinal >= consts.length) {
            throw new IllegalArgumentException(
                "enum ordinal " + ordinal + " out of range for " + fieldNames[i]
                    + " on " + recordType.getName() + " (max " + (consts.length - 1) + ")");
        }
        return consts[ordinal];
    }

    /**
     * Reconstruct a record from positional values. Uses
     * {@link MethodHandle#invokeWithArguments} so primitives are boxed once
     * during decode (the design accepts this — decode is paging-cool, not the
     * hot write path).
     */
    @SuppressWarnings("unchecked")
    protected @NotNull R construct(@NotNull Object[] values) {
        try {
            return (R) constructor.invokeWithArguments(values);
        } catch (Throwable t) {
            throw new IllegalStateException(
                "decode failed for " + recordType.getName(), t);
        }
    }

    /**
     * Allocate a constructor-argument array of {@link #totalComponentCount}
     * positions, pre-filled with the default for the corresponding component
     * type at each transient position (0 for primitives, null for references).
     * Per-format decoders then overwrite the persistent positions via
     * {@link #recordPositionAt}.
     */
    protected @NotNull Object[] freshArgsArray() {
        Object[] args = new Object[totalComponents];
        // The default 0/null for any unfilled slot is fine because:
        // - persistent slots are written by the decoder.
        // - transient slots rely on the record's compact constructor to
        //   coerce nulls to safe defaults (e.g. SessionRecord.sessionBlobs
        //   null -> List.of()).
        return args;
    }

    /** Resolve the canonical constructor from its component-order parameter types. */
    private static @NotNull MethodHandle canonicalConstructorHandle(
            @NotNull Class<?> recordType,
            @NotNull Class<?>[] paramTypes) throws Throwable {
        // Record canonical constructors are public; grim-internal's own lookup
        // resolves them. Avoid privateLookupIn on the api record class — it is
        // loaded by the loader's stub classloader, and a cross-classloader
        // privateLookupIn is not full-privilege (the trap Accessors hit under
        // Paper's reflection-rewriter on a Mojang-mapped server).
        return MethodHandles.lookup()
                .findConstructor(recordType, java.lang.invoke.MethodType.methodType(void.class, paramTypes));
    }

    /** Look up field index by name, or throw with the record + field for diagnostics. */
    protected int requireIndex(@NotNull String name) {
        Integer i = fieldIndex.get(name);
        if (i == null) {
            throw new IllegalArgumentException(
                "no such field on " + recordType.getName() + ": " + name);
        }
        return i;
    }

    /**
     * Public accessor for the per-field type tag, used by per-format helper
     * classes (e.g. {@code BsonDecodeHelper}).
     */
    public @NotNull TypeTag typeTagAt(int i) { return typeTags[i]; }

    /**
     * Public accessor for the per-field encoded name.
     */
    public @NotNull String fieldNameAt(int i) { return fieldNames[i]; }

    /** Number of persistent fields on this record. */
    public int fieldCount() { return fieldNames.length; }

    /**
     * Resolve a decoded raw value: missing non-nullable throws; missing
     * nullable returns null; missing nullable on a primitive type tag is
     * impossible (the introspection rejects {@code @Nullable} on primitives).
     * <p>
     * Exception: a non-nullable primitive-long field with
     * {@link ac.grim.grimac.api.storage.codec.MergeMode#PRESERVE_ON_NON_SENTINEL}
     * substitutes its sentinel value for null/missing input. Lets v2
     * read paths handle rows that legacy backends wrote with the
     * field absent or as explicit SQL/BSON NULL (e.g. legacy Mongo
     * sessions with {@code closed_at:null} representing OPEN, or SQL
     * rows imported before the field-level migration ran).
     */
    public @Nullable Object requireFieldOrDefault(int i, @Nullable Object value) {
        if (value != null) return value;
        if (nullable[i]) return null;
        // Sentinel-aware primitive-long fallback. We only check this on
        // the null path because the typical decode hits a non-null
        // value with no extra branching cost.
        FieldDef def = shape.fields().get(i);
        if (def.mergeMode() == ac.grim.grimac.api.storage.codec.MergeMode.PRESERVE_ON_NON_SENTINEL) {
            return def.sentinelValue();
        }
        throw new IllegalStateException(
            "missing non-nullable field " + fieldNames[i] + " on " + recordType.getName());
    }
}
