package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.Codec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;

/**
 * Multi-tenant key/value: many {@code (scope, scopeKey)} tenants, each
 * owning a map of string keys to value records. Replaces the "one document
 * per setting" anti-pattern in {@code grim_settings}.
 *
 * <p><b>Key grammar (portable across backends):</b> keys must be non-empty
 * strings of any UTF-8 characters except literal {@code .} and may not
 * start with {@code $}. The restriction is driven by the Mongo adapter's
 * {@code $set: {"values.<key>": ...}} field-path syntax (Mongo treats
 * {@code .} as a path separator and reserves {@code $} as an operator
 * prefix); other backends inherit the same rule so an extension's KV
 * Kind round-trips cleanly when the operator swaps backends. Adapter
 * implementations reject illegal keys at the operation boundary with
 * {@link IllegalArgumentException}.
 *
 * @param <S> the scope type (typically an enum like {@code PlayerScope})
 * @param <V> the value record type
 */
@ApiStatus.Experimental
public record KeyValueScoped<S, V>(
        @NotNull String name,
        @NotNull Class<S> scopeType,
        @NotNull Class<V> valueType,
        @NotNull Codec<V> valueCodec,
        @NotNull EnumSet<Capability> requiredCapabilities,
        @NotNull EnumSet<Capability> optionalCapabilities)
        implements DataKind<KeyValueEvent<S, V>, KeyValueRecord<S, V>> {

    public KeyValueScoped {
        requiredCapabilities = EnumSet.copyOf(requiredCapabilities);
        optionalCapabilities = EnumSet.copyOf(optionalCapabilities);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override public @NotNull Class<KeyValueEvent<S, V>> eventType() {
        return (Class) KeyValueEvent.class;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override public @NotNull Class<KeyValueRecord<S, V>> recordType() {
        return (Class) KeyValueRecord.class;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override public @NotNull Codec<KeyValueRecord<S, V>> codec() {
        throw new UnsupportedOperationException("KeyValueScoped uses valueCodec per-entry; container codec is synthesized by the adapter");
    }

    public static <S, V> @NotNull Builder<S, V> builder() {
        return new Builder<>();
    }

    public static final class Builder<S, V> {
        private String name;
        private Class<S> scopeType;
        private Class<V> valueType;
        private Codec<V> valueCodec;
        private final EnumSet<Capability> required = EnumSet.of(Capability.KIND_KV_SCOPED);
        private final EnumSet<Capability> optional = EnumSet.noneOf(Capability.class);

        public @NotNull Builder<S, V> name(@NotNull String name) { this.name = name; return this; }
        public @NotNull Builder<S, V> scope(@NotNull Class<S> t) { this.scopeType = t; return this; }
        public @NotNull Builder<S, V> value(@NotNull Class<V> t, @NotNull Codec<V> c) {
            this.valueType = t; this.valueCodec = c; return this;
        }
        public @NotNull Builder<S, V> requireCapability(@NotNull Capability... caps) {
            for (Capability c : caps) required.add(c); return this;
        }
        public @NotNull Builder<S, V> optionalCapability(@NotNull Capability... caps) {
            for (Capability c : caps) optional.add(c); return this;
        }

        public @NotNull KeyValueScoped<S, V> build() {
            if (name == null) throw new IllegalStateException("name");
            if (scopeType == null) throw new IllegalStateException("scope");
            if (valueType == null || valueCodec == null) throw new IllegalStateException("value");
            return new KeyValueScoped<>(name, scopeType, valueType, valueCodec, required, optional);
        }
    }
}
