package ac.grim.grimac.internal.storage.codec.bson;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.codec.FieldKind;
import ac.grim.grimac.internal.storage.codec.MethodHandleCodec;
import org.bson.BsonWriter;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Mongo timeseries variant of the BSON codec.
 * <p>
 * Layout:
 * <ul>
 *   <li>{@code _v} (codec version) first.</li>
 *   <li>{@link FieldKind#TIMESTAMP} field at the top level as BSON Date
 *       under its declared name (the collection's configured {@code timeField}).</li>
 *   <li>{@link FieldKind#PARTITION} fields as keys of a single
 *       {@code meta} sub-document (the collection's configured {@code metaField}).</li>
 *   <li>{@link FieldKind#ID}, {@link FieldKind#INDEXED}, {@link FieldKind#VALUE},
 *       and {@link FieldKind#SEARCHABLE} fields at the top level beside the
 *       timestamp.</li>
 * </ul>
 * Decode reverses the layout.
 */
@ApiStatus.Internal
public final class BsonTsCodecImpl<R> extends MethodHandleCodec<R> implements BsonTsCodec<R> {

    private static final String META_KEY = "meta";

    private final @NotNull FieldKind[] kinds;

    public BsonTsCodecImpl(@NotNull Class<R> recordType, @NotNull EncodeShape shape) {
        super(recordType, shape);
        int n = shape.fields().size();
        this.kinds = new FieldKind[n];
        for (int i = 0; i < n; i++) {
            kinds[i] = shape.fields().get(i).kind();
        }
        if (shape.timestampField() == null) {
            throw new IllegalArgumentException(
                "BsonTsCodec requires a @Timestamp field on " + recordType.getName());
        }
    }

    /** The collection's configured {@code timeField} name — surfaced for adapter introspection. */
    public @NotNull String timestampField() {
        return java.util.Objects.requireNonNull(shape().timestampField());
    }

    @Override
    public void encode(@NotNull R record, @NotNull BsonWriter w) {
        w.writeStartDocument();
        w.writeInt32(BsonCodecImpl.VERSION_FIELD, version());

        // Emit non-partition fields at the top level first (id, timestamp,
        // indexed, value, searchable). Partition fields go inside meta below.
        for (int i = 0; i < fieldNames.length; i++) {
            if (kinds[i] == FieldKind.PARTITION) continue;
            writeField(record, i, w, /*asDate*/ kinds[i] == FieldKind.TIMESTAMP);
        }

        // meta sub-document for partition fields.
        w.writeName(META_KEY);
        w.writeStartDocument();
        for (int i = 0; i < fieldNames.length; i++) {
            if (kinds[i] != FieldKind.PARTITION) continue;
            writeField(record, i, w, /*asDate*/ false);
        }
        w.writeEndDocument();

        w.writeEndDocument();
    }

    private void writeField(@NotNull R record, int i, @NotNull BsonWriter w, boolean asDate) {
        String name = fieldNames[i];
        switch (typeTags[i]) {
            case INT  -> w.writeInt32(name, readInt(record, i));
            case LONG -> {
                long v = readLong(record, i);
                if (asDate) w.writeDateTime(name, v);
                else        w.writeInt64(name, v);
            }
            case DOUBLE,
                 FLOAT -> w.writeDouble(name, readDouble(record, i));
            case ENUM  -> w.writeInt32(name, readEnumOrdinal(record, i));
            case BOOLEAN -> {
                Boolean v = (Boolean) readRef(record, i);
                if (v != null) w.writeBoolean(name, v);
                else if (!nullable[i]) throwMissingNonNullable(i);
            }
            case STRING -> {
                String s = (String) readRef(record, i);
                if (s != null) w.writeString(name, s);
                else if (!nullable[i]) throwMissingNonNullable(i);
            }
            case BYTES -> {
                byte[] b = (byte[]) readRef(record, i);
                if (b != null) w.writeBinaryData(name, new org.bson.BsonBinary(b));
                else if (!nullable[i]) throwMissingNonNullable(i);
            }
            case UUID -> {
                UUID u = (UUID) readRef(record, i);
                if (u != null) w.writeBinaryData(name, BsonBinaries.uuidBinary(u));
                else if (!nullable[i]) throwMissingNonNullable(i);
            }
            case FLOAT_ARRAY -> {
                float[] v = (float[]) readRef(record, i);
                if (v != null) BsonBinaries.writeFloatArray(name, v, w);
                else if (!nullable[i]) throwMissingNonNullable(i);
            }
            case NESTED_SEALED -> {
                Object v = readRef(record, i);
                if (v != null) {
                    throw new UnsupportedOperationException(
                        "NESTED_SEALED encoding not yet implemented (field " + name + "); ships with Phase 6");
                } else if (!nullable[i]) {
                    throwMissingNonNullable(i);
                }
            }
        }
    }

    private void throwMissingNonNullable(int i) {
        throw new IllegalStateException(
            "null value for non-nullable field " + fieldNames[i] + " on " + recordType().getName());
    }

    @Override
    public @NotNull R decode(@NotNull Document source) {
        // Same @Transient-aware shape as BsonCodecImpl.decode.
        Document meta = source.get(META_KEY, Document.class);
        Object[] args = freshArgsArray();
        for (int i = 0; i < fieldNames.length; i++) {
            Document src = (kinds[i] == FieldKind.PARTITION) ? meta : source;
            Object raw = src == null ? null : src.get(fieldNames[i]);
            args[recordPositionAt(i)] = BsonDecodeHelper.decodeField(this, i, raw);
        }
        return construct(args);
    }
}
