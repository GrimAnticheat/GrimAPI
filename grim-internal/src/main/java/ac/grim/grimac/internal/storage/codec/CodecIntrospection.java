package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.EncodeShape.FieldDef;
import ac.grim.grimac.api.storage.codec.FieldKind;
import ac.grim.grimac.api.storage.codec.Id;
import ac.grim.grimac.api.storage.codec.Indexed;
import ac.grim.grimac.api.storage.codec.InsertOnly;
import ac.grim.grimac.api.storage.codec.MergeMax;
import ac.grim.grimac.api.storage.codec.MergeMin;
import ac.grim.grimac.api.storage.codec.MergeMode;
import ac.grim.grimac.api.storage.codec.Name;
import ac.grim.grimac.api.storage.codec.Nullable;
import ac.grim.grimac.api.storage.codec.Partition;
import ac.grim.grimac.api.storage.codec.Persistent;
import ac.grim.grimac.api.storage.codec.PreserveOnNonNull;
import ac.grim.grimac.api.storage.codec.SearchType;
import ac.grim.grimac.api.storage.codec.Searchable;
import ac.grim.grimac.api.storage.codec.Sentinel;
import ac.grim.grimac.api.storage.codec.Timestamp;
import ac.grim.grimac.api.storage.codec.Transient;
import ac.grim.grimac.api.storage.codec.Value;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One-shot inspection of a {@link Persistent} record class. Walks the record
 * components, validates role annotations, derives the {@link EncodeShape}
 * the rest of the codec layer needs. Called once per record class at startup;
 * cached forever by {@link MethodHandleCodecFactory}.
 * <p>
 * The validator is strict — every disallowed combination becomes an
 * {@link IllegalArgumentException} at registration time so misconfigured
 * records fail at startup, never at first write.
 */
@ApiStatus.Internal
public final class CodecIntrospection {

    private CodecIntrospection() {}

    /**
     * Inspect {@code recordType}, validate annotations, return its shape.
     * Builtin records resolve from the build-time capture ({@link CapturedBindings})
     * because Allatori strips the {@code RecordComponents} attribute, so the
     * reflective path below cannot run on them at runtime. Extension records and
     * un-obfuscated runs (where the attribute is intact) fall through to it.
     *
     * @throws IllegalArgumentException on any spec violation: not a record,
     *         missing {@link Persistent}, missing or duplicate {@link Id},
     *         multiple {@link Timestamp} fields, {@link Nullable} on a
     *         primitive component, boxed numeric on a primitive type tag,
     *         {@link Searchable} with an incompatible Java type, or
     *         {@link Name} value that's already taken by another component.
     */
    public static @NotNull EncodeShape inspect(@NotNull Class<?> recordType) {
        EncodeShape captured = CapturedBindings.shape(recordType);
        if (captured != null) return captured;
        return inspectReflective(recordType);
    }

    /**
     * Reflective shape derivation — the canonical validator. Reads the record's
     * components and codec annotations directly; used for extension records,
     * un-obfuscated runs, and the build-time capture tool (which feeds the
     * captured fast path in {@link #inspect}). Requires the live
     * {@code RecordComponents} attribute. Public so the build-time capture tool
     * can force the reflective path regardless of any resource on its classpath.
     */
    public static @NotNull EncodeShape inspectReflective(@NotNull Class<?> recordType) {
        if (!recordType.isRecord()) {
            throw new IllegalArgumentException("not a record: " + recordType.getName());
        }
        Persistent persistent = recordType.getAnnotation(Persistent.class);
        if (persistent == null) {
            throw new IllegalArgumentException("missing @Persistent on " + recordType.getName());
        }

        RecordComponent[] components = recordType.getRecordComponents();
        List<FieldDef> fields = new ArrayList<>(components.length);
        String idField = null;
        String timestampField = null;
        List<String> partitionFields = new ArrayList<>();
        List<String> indexedFields = new ArrayList<>();
        List<String> searchableFields = new ArrayList<>();

        for (int componentIndex = 0; componentIndex < components.length; componentIndex++) {
            RecordComponent c = components[componentIndex];
            // @Transient components are skipped entirely — they're memory-only
            // (e.g. SessionRecord.sessionBlobs before the session blob feature ships).
            // The codec emits no field for them; decode reconstructs them via
            // the canonical constructor's natural default (null/empty for
            // collections, 0/false for primitives — relies on the record's
            // compact constructor to coerce nulls to empty defaults).
            if (c.isAnnotationPresent(Transient.class)) {
                validateTransient(recordType, c);
                continue;
            }

            String name = encodedName(c);
            FieldKind kind = roleOf(c, recordType);
            boolean nullable = isNullable(c);
            Class<?> javaType = c.getType();

            // Strict checks: nullable on primitive is meaningless and dangerous.
            if (nullable && javaType.isPrimitive()) {
                throw new IllegalArgumentException(
                    recordType.getName() + "." + c.getName() + ": @Nullable on primitive type " + javaType);
            }
            // @Nullable on enum requires a nullable-aware encode path the BSON
            // codecs don't ship yet (they always extract ordinal). Reject for
            // now; lift when nullable-enum encode lands (likely Phase 6 with
            // the discriminator-byte path for sealed types).
            if (nullable && javaType.isEnum()) {
                throw new IllegalArgumentException(
                    recordType.getName() + "." + c.getName()
                        + ": @Nullable on enum not yet supported by the BSON codec");
            }
            // Boxed numeric paired with a primitive TypeTag would NPE at encode
            // through the primitive accessor path. Reject upfront.
            if (isBoxedNumeric(javaType)) {
                throw new IllegalArgumentException(
                    recordType.getName() + "." + c.getName() + ": boxed numeric (" + javaType.getSimpleName()
                        + ") not supported as a persistent field — use the primitive form");
            }

            SearchType searchType = null;
            int vectorDim = 0;
            Searchable searchable = c.getAnnotation(Searchable.class);
            if (searchable != null) {
                searchType = searchable.value();
                vectorDim = searchable.dimension();
                validateSearchableType(recordType, c, searchType, vectorDim);
                searchableFields.add(name);
            }

            switch (kind) {
                case ID -> {
                    if (idField != null) {
                        throw new IllegalArgumentException(
                            recordType.getName() + ": multiple @Id components");
                    }
                    idField = name;
                }
                case TIMESTAMP -> {
                    if (timestampField != null) {
                        throw new IllegalArgumentException(
                            recordType.getName() + ": multiple @Timestamp components");
                    }
                    if (javaType != long.class) {
                        throw new IllegalArgumentException(
                            recordType.getName() + "." + c.getName()
                                + ": @Timestamp requires long (epoch millis), got " + javaType.getName());
                    }
                    timestampField = name;
                }
                case PARTITION -> partitionFields.add(name);
                case INDEXED -> indexedFields.add(name);
                default -> { /* VALUE / SEARCHABLE — no top-level index slot */ }
            }

            MergeResolution merge = resolveMergeMode(recordType, c, kind, nullable, javaType);

            fields.add(new FieldDef(name, javaType, kind, nullable, searchType, vectorDim, componentIndex,
                merge.mode(), merge.sentinel()));
        }

        if (idField == null) {
            throw new IllegalArgumentException(
                recordType.getName() + ": @Persistent record must declare exactly one @Id component");
        }

        // Check for encoded-name collisions (two components naming the same on-disk key).
        long uniqueNames = fields.stream().map(FieldDef::name).distinct().count();
        if (uniqueNames != fields.size()) {
            throw new IllegalArgumentException(
                recordType.getName() + ": encoded field names must be unique — check @Name overrides");
        }

        // Reject the names the codec layer reserves for its own use.
        for (FieldDef f : fields) {
            if (RESERVED_FIELD_NAMES.contains(f.name())) {
                throw new IllegalArgumentException(
                    recordType.getName() + ": field name '" + f.name()
                        + "' is reserved by the codec layer — pick a different @Name or rename the component");
            }
            // The `meta` sub-document name is reserved on timeseries layouts.
            // Reject it at every layout to keep records portable across Kinds.
            if (RESERVED_TIMESERIES_TOP_LEVEL_NAMES.contains(f.name())) {
                throw new IllegalArgumentException(
                    recordType.getName() + ": field name '" + f.name()
                        + "' is reserved for the timeseries meta sub-document — pick a different @Name");
            }
        }

        // totalComponentCount is the record's FULL component arity (persistent
        // + transient). The decoder uses it to size the constructor argument
        // array correctly when there are interspersed or trailing transient
        // fields. Derived from the reflective component array, not from
        // max(recordIndex)+1 (which undercounts trailing transients).
        return new EncodeShape(idField, timestampField, partitionFields, indexedFields, searchableFields, fields,
                /*totalComponentCount=*/ components.length);
    }

    /**
     * Names the BSON codec stamps itself; conflicting with one of these would
     * cause the encoded record to be ambiguous on the wire.
     */
    private static final java.util.Set<String> RESERVED_FIELD_NAMES = java.util.Set.of(
        "_v",       // top-level codec version
        "_id"       // Mongo's primary key — Entity adapters use $setOnInsert {_id}; a
                    // codec-emitted _id collides with Mongo's immutable-id rule.
    );

    /**
     * Names that conflict with the timeseries layout's emitted structural
     * documents at the top level (currently just the {@code meta}
     * sub-document holding partition fields).
     */
    private static final java.util.Set<String> RESERVED_TIMESERIES_TOP_LEVEL_NAMES = java.util.Set.of(
        "meta"
    );

    /**
     * Validate that a {@code @Transient} component doesn't also carry a
     * persistence-role annotation. Catches "marked transient but also
     * marked @Id" type bugs at registration.
     */
    private static void validateTransient(@NotNull Class<?> owner, @NotNull RecordComponent c) {
        if (c.isAnnotationPresent(Id.class)
                || c.isAnnotationPresent(Partition.class)
                || c.isAnnotationPresent(Timestamp.class)
                || c.isAnnotationPresent(Indexed.class)
                || c.isAnnotationPresent(Searchable.class)
                || c.isAnnotationPresent(Value.class)) {
            throw new IllegalArgumentException(
                owner.getName() + "." + c.getName()
                    + ": @Transient cannot combine with any persistence-role annotation");
        }
    }

    /**
     * Determine the per-field merge mode from the (mutually exclusive)
     * merge annotation set. Validates:
     * <ul>
     *   <li>At most one merge annotation per component.</li>
     *   <li>@PreserveOnNonNull requires a nullable reference type — the
     *       semantics rely on null-vs-non-null comparison.</li>
     *   <li>@MergeMax / @MergeMin require a numeric type — GREATEST/LEAST
     *       only make sense over comparable numbers.</li>
     *   <li>@InsertOnly forbidden on @Id (already insert-once by
     *       Mongo's _id immutability and SQL's PRIMARY KEY contract;
     *       redundant + confusing).</li>
     * </ul>
     */
    private static @NotNull MergeResolution resolveMergeMode(@NotNull Class<?> owner, @NotNull RecordComponent c,
                                                             @NotNull FieldKind kind, boolean nullable,
                                                             @NotNull Class<?> javaType) {
        boolean insertOnly = c.isAnnotationPresent(InsertOnly.class);
        boolean preserve   = c.isAnnotationPresent(PreserveOnNonNull.class);
        boolean max        = c.isAnnotationPresent(MergeMax.class);
        boolean min        = c.isAnnotationPresent(MergeMin.class);
        Sentinel sentinel  = c.getAnnotation(Sentinel.class);
        int count = (insertOnly ? 1 : 0) + (preserve ? 1 : 0) + (max ? 1 : 0) + (min ? 1 : 0)
                  + (sentinel != null ? 1 : 0);
        if (count == 0) return new MergeResolution(MergeMode.OVERWRITE, 0L);
        if (count > 1) {
            throw new IllegalArgumentException(
                owner.getName() + "." + c.getName()
                    + ": at most one of @InsertOnly / @PreserveOnNonNull / @Sentinel / @MergeMax / @MergeMin");
        }
        if (insertOnly) {
            if (kind == FieldKind.ID) {
                throw new IllegalArgumentException(
                    owner.getName() + "." + c.getName()
                        + ": @InsertOnly is redundant on @Id (PK is insert-once by definition)");
            }
            return new MergeResolution(MergeMode.INSERT_ONLY, 0L);
        }
        if (preserve) {
            if (!nullable || javaType.isPrimitive()) {
                throw new IllegalArgumentException(
                    owner.getName() + "." + c.getName()
                        + ": @PreserveOnNonNull requires a @Nullable reference-typed field");
            }
            return new MergeResolution(MergeMode.PRESERVE_ON_NON_NULL, 0L);
        }
        if (sentinel != null) {
            if (javaType != long.class) {
                throw new IllegalArgumentException(
                    owner.getName() + "." + c.getName()
                        + ": @Sentinel requires primitive long, got " + javaType.getName());
            }
            if (kind == FieldKind.ID) {
                throw new IllegalArgumentException(
                    owner.getName() + "." + c.getName()
                        + ": @Sentinel is redundant on @Id (PK is insert-once by definition)");
            }
            return new MergeResolution(MergeMode.PRESERVE_ON_NON_SENTINEL, sentinel.value());
        }
        if (max || min) {
            if (!isNumericForMerge(javaType)) {
                throw new IllegalArgumentException(
                    owner.getName() + "." + c.getName()
                        + ": @MergeMax/@MergeMin require a numeric type, got " + javaType.getName());
            }
            return new MergeResolution(max ? MergeMode.MAX : MergeMode.MIN, 0L);
        }
        // Unreachable — count >= 1 was handled above.
        return new MergeResolution(MergeMode.OVERWRITE, 0L);
    }

    /** Internal tuple — codec introspection result for one component's merge metadata. */
    private record MergeResolution(@NotNull MergeMode mode, long sentinel) {}

    private static boolean isNumericForMerge(@NotNull Class<?> t) {
        return t == long.class || t == int.class || t == double.class || t == float.class;
    }

    /** Determine the canonical field role; @Searchable alone defaults to VALUE. */
    private static @NotNull FieldKind roleOf(@NotNull RecordComponent c, @NotNull Class<?> owner) {
        boolean isId        = c.isAnnotationPresent(Id.class);
        boolean isPartition = c.isAnnotationPresent(Partition.class);
        boolean isTimestamp = c.isAnnotationPresent(Timestamp.class);
        boolean isIndexed   = c.isAnnotationPresent(Indexed.class);
        boolean isValue     = c.isAnnotationPresent(Value.class);

        int primary = (isId ? 1 : 0) + (isPartition ? 1 : 0) + (isTimestamp ? 1 : 0);
        if (primary > 1) {
            throw new IllegalArgumentException(
                owner.getName() + "." + c.getName() + ": cannot combine @Id / @Partition / @Timestamp");
        }

        if (isId)        return FieldKind.ID;
        if (isPartition) return FieldKind.PARTITION;
        if (isTimestamp) return FieldKind.TIMESTAMP;
        if (isIndexed)   return FieldKind.INDEXED;
        if (isValue)     return FieldKind.VALUE;

        // Bare component with no role annotation: tolerate as VALUE so the codec
        // ergonomics stay light. Authors can be explicit by adding @Value.
        return FieldKind.VALUE;
    }

    /** Read {@link Name}'s explicit override, else snake_case the Java component name. */
    private static @NotNull String encodedName(@NotNull RecordComponent c) {
        Name override = c.getAnnotation(Name.class);
        if (override != null) {
            String v = override.value();
            if (v == null || v.isEmpty()) {
                throw new IllegalArgumentException("@Name value must be non-empty on " + c.getName());
            }
            return v;
        }
        return snakeCase(c.getName());
    }

    /** Storage-owned runtime {@link Nullable} marker. JetBrains @Nullable is NOT runtime-retained and is ignored. */
    private static boolean isNullable(@NotNull RecordComponent c) {
        return c.isAnnotationPresent(Nullable.class);
    }

    private static boolean isBoxedNumeric(@NotNull Class<?> t) {
        return t == Integer.class || t == Long.class || t == Double.class || t == Float.class
                || t == Short.class || t == Byte.class;
    }

    private static void validateSearchableType(
            @NotNull Class<?> owner,
            @NotNull RecordComponent c,
            @NotNull SearchType st,
            int vectorDim) {
        Class<?> t = c.getType();
        switch (st) {
            case KEYWORD, KEYWORD_PREFIX, TEXT -> {
                if (t != String.class) {
                    throw new IllegalArgumentException(
                        owner.getName() + "." + c.getName() + ": @Searchable(" + st + ") requires String, got " + t.getName());
                }
            }
            case NUMERIC -> {
                if (t != int.class && t != long.class && t != double.class && t != float.class) {
                    throw new IllegalArgumentException(
                        owner.getName() + "." + c.getName() + ": @Searchable(NUMERIC) requires int/long/double/float primitive, got " + t.getName());
                }
            }
            case VECTOR -> {
                if (t != float[].class) {
                    throw new IllegalArgumentException(
                        owner.getName() + "." + c.getName() + ": @Searchable(VECTOR) requires float[], got " + t.getName());
                }
                if (vectorDim <= 0) {
                    throw new IllegalArgumentException(
                        owner.getName() + "." + c.getName() + ": @Searchable(VECTOR) requires dimension > 0");
                }
            }
        }
    }

    /** snake_case the camelCase Java component name. {@code occurredEpochMs} -> {@code occurred_epoch_ms}. */
    public static @NotNull String snakeCase(@NotNull String camel) {
        StringBuilder sb = new StringBuilder(camel.length() + 4);
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) sb.append('_');
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Find a RecordComponent by name (Java field name, not snake_case). */
    public static java.lang.reflect.RecordComponent componentByJavaName(@NotNull Class<?> recordType, @NotNull String javaName) {
        return Arrays.stream(recordType.getRecordComponents())
                .filter(rc -> rc.getName().equals(javaName))
                .findFirst().orElse(null);
    }
}
