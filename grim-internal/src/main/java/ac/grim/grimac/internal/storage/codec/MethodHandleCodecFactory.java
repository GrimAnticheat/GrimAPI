package ac.grim.grimac.internal.storage.codec;

import ac.grim.grimac.api.storage.codec.Codec;
import ac.grim.grimac.api.storage.codec.Codecs;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link Codecs.Provider} that builds a {@link MethodHandleCodec} per
 * record class. Caches one base codec per record type; per-format codec
 * objects (Bson, BsonTs, Jdbc, Redis) are produced lazily by the format
 * subpackages from the same base when their backend is wired.
 * <p>
 * Install at storage bootstrap with {@link #installAsDefaultProvider()}.
 */
@ApiStatus.Internal
public final class MethodHandleCodecFactory implements Codecs.Provider {

    private static final MethodHandleCodecFactory INSTANCE = new MethodHandleCodecFactory();

    private final ConcurrentMap<Class<?>, MethodHandleCodec<?>> cache = new ConcurrentHashMap<>();

    private MethodHandleCodecFactory() {}

    /** Idempotent install. Safe to call multiple times. */
    public static void installAsDefaultProvider() {
        Codecs.install(INSTANCE);
    }

    public static @NotNull MethodHandleCodecFactory get() { return INSTANCE; }

    @Override
    @SuppressWarnings("unchecked")
    public <R> @NotNull Codec<R> create(@NotNull Class<R> recordType) {
        return (Codec<R>) cache.computeIfAbsent(recordType, MethodHandleCodecFactory::build);
    }

    /** Get the codec as its MethodHandleCodec base, for format subclasses that wrap it. */
    @SuppressWarnings("unchecked")
    public <R> @NotNull MethodHandleCodec<R> baseCodecFor(@NotNull Class<R> recordType) {
        return (MethodHandleCodec<R>) cache.computeIfAbsent(recordType, MethodHandleCodecFactory::build);
    }

    private static @NotNull MethodHandleCodec<?> build(@NotNull Class<?> recordType) {
        EncodeShape shape = CodecIntrospection.inspect(recordType);
        return new MethodHandleCodec<>(recordType, shape);
    }
}
