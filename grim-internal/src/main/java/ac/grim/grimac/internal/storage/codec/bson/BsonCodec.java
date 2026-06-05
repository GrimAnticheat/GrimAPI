package ac.grim.grimac.internal.storage.codec.bson;

import ac.grim.grimac.api.storage.codec.Codec;
import org.bson.BsonWriter;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Per-format codec interface for regular Mongo collection layouts —
 * every persistent field at the top level of the BSON document. Used by
 * Entity and KeyValueScoped Kinds; EventStream Kinds on Mongo timeseries
 * use {@link BsonTsCodec} instead.
 *
 * <p>Encode is streaming through {@link BsonWriter}: the hot path is
 * allocation-free for primitive fields. Decode is from {@link Document}
 * because that's what the standard driver read path returns — decode
 * isn't on the write hot path so the {@code Document} allocation is OK.
 *
 * @param <R> the persistent record type
 */
@ApiStatus.Internal
public interface BsonCodec<R> extends Codec<R> {

    /**
     * Stream the record into {@code writer}, starting a fresh document.
     * The first emitted field is always {@code _v} (codec version) so the
     * schema-evolution upgrader chain on read can route by version.
     * <p>
     * Caller manages the underlying {@code BsonBinaryWriter} +
     * {@code BasicOutputBuffer}; they're typically pooled per per-handler
     * thread for zero-allocation writes.
     * <p>
     * Equivalent to:
     * <pre>{@code
     *   writer.writeStartDocument();
     *   encodeFields(record, writer);
     *   writer.writeEndDocument();
     * }</pre>
     */
    void encode(@NotNull R record, @NotNull BsonWriter writer);

    /**
     * Stream only the persistent fields (including {@code _v}) into
     * {@code writer}, with NO surrounding {@code writeStartDocument} /
     * {@code writeEndDocument} calls. The caller is responsible for
     * starting and ending the document.
     * <p>
     * Used by adapters that need to inject extra fields adjacent to the
     * codec-owned fields — e.g. a Mongo Entity adapter writing
     * {@code current_name_lower} companion fields for a
     * case-insensitive-prefix index after the codec's String field but
     * before the document is closed.
     */
    void encodeFields(@NotNull R record, @NotNull BsonWriter writer);

    /**
     * Read the value of a persistent field from the record at the
     * EncodeShape's field index, returned as a boxed reference. Used by
     * adapters that need access to the in-record value for derived
     * sidecar fields (e.g. lowercased companion columns) without
     * re-decoding the encoded BSON. Allocates only for primitive fields.
     *
     * @param record       the source record (must not be null)
     * @param fieldIndex   the persistent field index (0..shape.fields().size() - 1)
     * @return the field's value as a boxed reference; {@code null} for a
     *         null reference-typed field
     */
    @org.jetbrains.annotations.Nullable Object readField(@NotNull R record, int fieldIndex);

    /** Construct a record from a previously-encoded {@link Document}. */
    @NotNull R decode(@NotNull Document source);

    /**
     * Construct a record from an already-typed positional argument
     * array. {@code argsByShapeIndex} must have one entry per
     * {@code shape().fields()} entry, in shape order; the value at
     * index {@code i} must be the Java value of the {@code i}-th
     * persistent field (boxed for primitives — the codec coerces back
     * on construct).
     * <p>
     * Used by non-BSON adapters (SQL, Redis hash) that decode each
     * column natively rather than through the BSON {@link Document}
     * path. Transient components get their default (0 / null) and the
     * compact constructor coerces.
     */
    @NotNull R decodeFromValues(@org.jetbrains.annotations.Nullable Object @NotNull [] argsByShapeIndex);
}
