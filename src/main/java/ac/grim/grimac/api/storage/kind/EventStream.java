package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.Codec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Append-only, time-ordered events with a small number of partition keys.
 * Canonical case: {@code grim_violations}. Backends map this to their
 * timeseries primitive: Mongo timeseries collection, Postgres
 * {@code PARTITION BY RANGE} (or TimescaleDB hypertable), Redis Streams.
 *
 * <p>The operation menu lives on
 * {@code ac.grim.grimac.api.storage.category.EventStreamCategory}, which a
 * registered EventStream is returned as.
 */
@ApiStatus.Experimental
public record EventStream<E, R>(
        @NotNull String name,
        @NotNull Class<E> eventType,
        @NotNull Supplier<E> newEvent,
        @NotNull Class<R> recordType,
        @NotNull Codec<R> codec,
        @NotNull Function<E, R> eventToRecord,
        @NotNull String timestampField,
        @NotNull List<String> partitionFields,
        @Nullable Duration retention,
        @NotNull Granularity granularity,
        @NotNull EnumSet<Capability> requiredCapabilities,
        @NotNull EnumSet<Capability> optionalCapabilities) implements DataKind<E, R> {

    public EventStream {
        partitionFields = List.copyOf(partitionFields);
        requiredCapabilities = EnumSet.copyOf(requiredCapabilities);
        optionalCapabilities = EnumSet.copyOf(optionalCapabilities);
    }

    public static <E, R> @NotNull Builder<E, R> builder() {
        return new Builder<>();
    }

    public static final class Builder<E, R> {
        private String name;
        private Class<E> eventType;
        private Supplier<E> newEvent;
        private Class<R> recordType;
        private Codec<R> codec;
        private Function<E, R> eventToRecord;
        private String timestampField;
        private List<String> partitionFields = List.of();
        private Duration retention;
        private Granularity granularity = Granularity.SECONDS;
        private final EnumSet<Capability> required = EnumSet.of(Capability.KIND_EVENT_STREAM);
        private final EnumSet<Capability> optional = EnumSet.noneOf(Capability.class);

        public @NotNull Builder<E, R> name(@NotNull String name) { this.name = name; return this; }
        public @NotNull Builder<E, R> event(@NotNull Class<E> t, @NotNull Supplier<E> f) {
            this.eventType = t; this.newEvent = f; return this;
        }
        public @NotNull Builder<E, R> record(@NotNull Class<R> t) { this.recordType = t; return this; }
        public @NotNull Builder<E, R> codec(@NotNull Codec<R> c) { this.codec = c; return this; }
        /** How to materialise the immutable record from a mutable ring-slot event. Called once per write in the handler thread. */
        public @NotNull Builder<E, R> eventToRecord(@NotNull Function<E, R> f) { this.eventToRecord = f; return this; }
        public @NotNull Builder<E, R> timestamp(@NotNull String field) { this.timestampField = field; return this; }
        public @NotNull Builder<E, R> partition(@NotNull String... fields) {
            this.partitionFields = List.of(fields); return this;
        }
        public @NotNull Builder<E, R> retention(@Nullable Duration d) { this.retention = d; return this; }
        public @NotNull Builder<E, R> granularity(@NotNull Granularity g) { this.granularity = g; return this; }
        public @NotNull Builder<E, R> requireCapability(@NotNull Capability... caps) {
            for (Capability c : caps) required.add(c); return this;
        }
        public @NotNull Builder<E, R> optionalCapability(@NotNull Capability... caps) {
            for (Capability c : caps) optional.add(c); return this;
        }

        public @NotNull EventStream<E, R> build() {
            if (name == null) throw new IllegalStateException("name");
            if (eventType == null || newEvent == null) throw new IllegalStateException("event");
            if (recordType == null) throw new IllegalStateException("record");
            if (codec == null) throw new IllegalStateException("codec");
            if (eventToRecord == null) throw new IllegalStateException("eventToRecord");
            if (timestampField == null) throw new IllegalStateException("timestamp");
            return new EventStream<>(name, eventType, newEvent, recordType, codec, eventToRecord,
                    timestampField, partitionFields, retention, granularity,
                    required, optional);
        }
    }
}
