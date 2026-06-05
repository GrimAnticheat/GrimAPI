package ac.grim.grimac.internal.storage.codec.bson;

import ac.grim.grimac.api.storage.codec.Codec;
import org.bson.BsonWriter;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Per-format codec interface for Mongo timeseries collection layouts.
 * Partition fields live inside a {@code meta} sub-document; the timestamp
 * field is at the top level as BSON Date; the id + value fields are at
 * the top level alongside. Used by EventStream Kinds when routed to a
 * Mongo 5+ timeseries collection.
 *
 * <p>Same encode/decode shape contract as {@link BsonCodec} — streaming
 * encode, {@link Document} decode.
 *
 * @param <R> the persistent record type
 */
@ApiStatus.Internal
public interface BsonTsCodec<R> extends Codec<R> {

    void encode(@NotNull R record, @NotNull BsonWriter writer);

    @NotNull R decode(@NotNull Document source);
}
