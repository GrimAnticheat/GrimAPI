package ac.grim.grimac.internal.storage.codec.bson;

import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.internal.storage.codec.CodecIntrospection;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-{@link BsonCodec}/{@link BsonTsCodec} cache. Backends call into here
 * when they need a codec for one of their stores; results live for the
 * lifetime of the JVM.
 * <p>
 * Note: {@link BsonCodecImpl} and {@link BsonTsCodecImpl} each carry their
 * own {@link ac.grim.grimac.internal.storage.codec.MethodHandleCodec} base.
 * That means a record class registered for both regular + timeseries layouts
 * builds its accessor tables twice — fine, ~1 ms each at startup.
 * <p>
 * TODO(phase 6): route through the public {@link ac.grim.grimac.api.storage.codec.Codecs.Provider}
 * SPI so an extension installing a ByteBuddy-backed provider also picks up the
 * format-specific codecs. Today the format codecs are constructed directly,
 * bypassing the provider. Acceptable while there's only one provider impl.
 */
@ApiStatus.Internal
public final class BsonCodecs {

    private static final ConcurrentMap<Class<?>, BsonCodecImpl<?>> REGULAR = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, BsonTsCodecImpl<?>> TIMESERIES = new ConcurrentHashMap<>();

    private BsonCodecs() {}

    @SuppressWarnings("unchecked")
    public static <R> @NotNull BsonCodec<R> regular(@NotNull Class<R> recordType) {
        return (BsonCodec<R>) REGULAR.computeIfAbsent(recordType, t -> {
            EncodeShape shape = CodecIntrospection.inspect(t);
            return new BsonCodecImpl<>(t, shape);
        });
    }

    @SuppressWarnings("unchecked")
    public static <R> @NotNull BsonTsCodec<R> timeseries(@NotNull Class<R> recordType) {
        return (BsonTsCodec<R>) TIMESERIES.computeIfAbsent(recordType, t -> {
            EncodeShape shape = CodecIntrospection.inspect(t);
            return new BsonTsCodecImpl<>(t, shape);
        });
    }
}
