package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.Codec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;

/**
 * Atomic integer counter keyed by {@code K}. Common operations are
 * {@code incrementBy} and {@code setIfHigher}; reads are non-blocking
 * single-key lookups. Integer-only (no floats — see open questions).
 */
@ApiStatus.Experimental
public record Counter<K>(
        @NotNull String name,
        @NotNull Class<K> keyType,
        @NotNull Codec<K> keyCodec,
        @NotNull EnumSet<Capability> requiredCapabilities,
        @NotNull EnumSet<Capability> optionalCapabilities)
        implements DataKind<CounterEvent<K>, Long> {

    public Counter {
        requiredCapabilities = EnumSet.copyOf(requiredCapabilities);
        optionalCapabilities = EnumSet.copyOf(optionalCapabilities);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override public @NotNull Class<CounterEvent<K>> eventType() {
        return (Class) CounterEvent.class;
    }

    @Override public @NotNull Class<Long> recordType() { return Long.class; }

    @Override public @NotNull Codec<Long> codec() {
        throw new UnsupportedOperationException("Counter records are primitive longs; no codec");
    }

    public static <K> @NotNull Builder<K> builder() {
        return new Builder<>();
    }

    public static final class Builder<K> {
        private String name;
        private Class<K> keyType;
        private Codec<K> keyCodec;
        private final EnumSet<Capability> required = EnumSet.of(Capability.KIND_COUNTER);
        private final EnumSet<Capability> optional = EnumSet.noneOf(Capability.class);

        public @NotNull Builder<K> name(@NotNull String name) { this.name = name; return this; }
        public @NotNull Builder<K> key(@NotNull Class<K> t, @NotNull Codec<K> c) {
            this.keyType = t; this.keyCodec = c; return this;
        }
        public @NotNull Builder<K> requireCapability(@NotNull Capability... caps) {
            for (Capability c : caps) required.add(c); return this;
        }

        public @NotNull Counter<K> build() {
            if (name == null) throw new IllegalStateException("name");
            if (keyType == null || keyCodec == null) throw new IllegalStateException("key");
            return new Counter<>(name, keyType, keyCodec, required, optional);
        }
    }
}
