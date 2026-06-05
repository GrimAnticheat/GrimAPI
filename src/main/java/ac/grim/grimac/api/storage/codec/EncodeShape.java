package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Declared physical shape of a record's encoded form. Produced by the codec
 * generator from {@link Persistent} introspection; consumed by per-backend
 * adapters to decide what to index, what columns to project, what BSON
 * structure to write.
 * <p>
 * {@code totalComponentCount} is the record's FULL component arity (persistent
 * + transient), needed to size the constructor argument array correctly on
 * decode. {@code max(recordIndex) + 1} would undercount when a record has
 * trailing transient components.
 */
@ApiStatus.Experimental
public record EncodeShape(
        @NotNull String idField,
        @Nullable String timestampField,
        @NotNull List<String> partitionFields,
        @NotNull List<String> indexedFields,
        @NotNull List<String> searchableFields,
        @NotNull List<FieldDef> fields,
        int totalComponentCount) {

    public EncodeShape {
        partitionFields = List.copyOf(partitionFields);
        indexedFields = List.copyOf(indexedFields);
        searchableFields = List.copyOf(searchableFields);
        fields = List.copyOf(fields);
        if (totalComponentCount < fields.size()) {
            throw new IllegalArgumentException(
                "totalComponentCount (" + totalComponentCount + ") < persistent field count (" + fields.size() + ")");
        }
    }

    public record FieldDef(
            @NotNull String name,
            @NotNull Class<?> javaType,
            @NotNull FieldKind kind,
            boolean nullable,
            @Nullable SearchType searchType,
            int vectorDimension,
            int recordIndex,
            @NotNull MergeMode mergeMode,
            long sentinelValue) {

        /**
         * 8-arg constructor for callers that don't need the
         * {@link MergeMode#PRESERVE_ON_NON_SENTINEL} sentinel value.
         * Defaults to {@code sentinelValue = 0L} (the conventional
         * "unset" value for epoch-ms longs).
         */
        public FieldDef(@NotNull String name, @NotNull Class<?> javaType, @NotNull FieldKind kind,
                        boolean nullable, @Nullable SearchType searchType, int vectorDimension, int recordIndex,
                        @NotNull MergeMode mergeMode) {
            this(name, javaType, kind, nullable, searchType, vectorDimension, recordIndex, mergeMode, 0L);
        }

        /**
         * Backwards-compatible constructor that defaults
         * {@link MergeMode#OVERWRITE} and {@code sentinelValue = 0L}.
         * Used by adapter-internal synthetic {@code FieldDef}s
         * (e.g. the SQL prefix-pattern bind param) that never go
         * through the codec introspection path.
         */
        public FieldDef(@NotNull String name, @NotNull Class<?> javaType, @NotNull FieldKind kind,
                        boolean nullable, @Nullable SearchType searchType, int vectorDimension, int recordIndex) {
            this(name, javaType, kind, nullable, searchType, vectorDimension, recordIndex, MergeMode.OVERWRITE, 0L);
        }

        /**
         * Position of this persistent field in the record's full component
         * list (which may include transient components). Decode uses this
         * to place the decoded value at the correct constructor parameter
         * slot.
         */
        public int recordIndex() { return recordIndex; }

        /**
         * Sentinel value associated with {@link MergeMode#PRESERVE_ON_NON_SENTINEL}.
         * Ignored for other merge modes. Captured from
         * {@link Sentinel @Sentinel} at codec-introspection time.
         */
        public long sentinelValue() { return sentinelValue; }
    }
}
