package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.Codec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Identity-keyed mutable record. One row per id; writes are atomic upserts
 * (see {@code MongoEntityAdapter}'s {@code $set}+{@code $setOnInsert} pattern).
 * Canonical cases: {@code grim_sessions}, {@code grim_players}, {@code grim_checks}.
 */
@ApiStatus.Experimental
public record Entity<ID, E, R>(
        @NotNull String name,
        @NotNull Class<E> eventType,
        @NotNull Supplier<E> newEvent,
        @NotNull Class<R> recordType,
        @NotNull Codec<R> codec,
        @NotNull Function<E, R> eventToRecord,
        @NotNull Class<ID> idType,
        @NotNull Function<R, ID> idOf,
        @NotNull List<IndexSpec> secondaryIndexes,
        @NotNull EnumSet<Capability> requiredCapabilities,
        @NotNull EnumSet<Capability> optionalCapabilities) implements DataKind<E, R> {

    public Entity {
        secondaryIndexes = List.copyOf(secondaryIndexes);
        requiredCapabilities = EnumSet.copyOf(requiredCapabilities);
        optionalCapabilities = EnumSet.copyOf(optionalCapabilities);
    }

    public static <ID, E, R> @NotNull Builder<ID, E, R> builder() {
        return new Builder<>();
    }

    public static final class Builder<ID, E, R> {
        private String name;
        private Class<E> eventType;
        private Supplier<E> newEvent;
        private Class<R> recordType;
        private Codec<R> codec;
        private Function<E, R> eventToRecord;
        private Class<ID> idType;
        private Function<R, ID> idOf;
        private final List<IndexSpec> indexes = new java.util.ArrayList<>();
        private final EnumSet<Capability> required = EnumSet.of(Capability.KIND_ENTITY);
        private final EnumSet<Capability> optional = EnumSet.noneOf(Capability.class);

        public @NotNull Builder<ID, E, R> name(@NotNull String name) { this.name = name; return this; }
        public @NotNull Builder<ID, E, R> event(@NotNull Class<E> t, @NotNull Supplier<E> f) {
            this.eventType = t; this.newEvent = f; return this;
        }
        public @NotNull Builder<ID, E, R> record(@NotNull Class<R> t) { this.recordType = t; return this; }
        public @NotNull Builder<ID, E, R> codec(@NotNull Codec<R> c) { this.codec = c; return this; }
        /** How to materialise the immutable record from a mutable ring-slot event. Called once per write in the handler thread. */
        public @NotNull Builder<ID, E, R> eventToRecord(@NotNull Function<E, R> f) { this.eventToRecord = f; return this; }
        public @NotNull Builder<ID, E, R> id(@NotNull Class<ID> t, @NotNull Function<R, ID> f) {
            this.idType = t; this.idOf = f; return this;
        }
        public @NotNull Builder<ID, E, R> secondaryIndex(@NotNull IndexSpec spec) {
            indexes.add(spec); return this;
        }
        public @NotNull Builder<ID, E, R> secondaryIndex(@NotNull String name, @NotNull String... fields) {
            indexes.add(IndexSpec.of(name, fields)); return this;
        }
        public @NotNull Builder<ID, E, R> requireCapability(@NotNull Capability... caps) {
            for (Capability c : caps) required.add(c); return this;
        }
        public @NotNull Builder<ID, E, R> optionalCapability(@NotNull Capability... caps) {
            for (Capability c : caps) optional.add(c); return this;
        }

        public @NotNull Entity<ID, E, R> build() {
            if (name == null) throw new IllegalStateException("name");
            if (eventType == null || newEvent == null) throw new IllegalStateException("event");
            if (recordType == null) throw new IllegalStateException("record");
            if (codec == null) throw new IllegalStateException("codec");
            if (eventToRecord == null) throw new IllegalStateException("eventToRecord");
            if (idType == null || idOf == null) throw new IllegalStateException("id");
            return new Entity<>(name, eventType, newEvent, recordType, codec, eventToRecord,
                    idType, idOf, indexes, required, optional);
        }
    }
}
