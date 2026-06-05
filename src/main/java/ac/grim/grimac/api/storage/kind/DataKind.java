package ac.grim.grimac.api.storage.kind;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.codec.Codec;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;

/**
 * Typed declaration of how a category's data is shaped and accessed. The
 * routing layer and per-backend adapters use this to decide physical layout:
 * Mongo timeseries vs partitioned SQL table vs Redis stream all flow from
 * the same {@link EventStream} declaration.
 * <p>
 * Sealed: every supported access pattern is a permit. New patterns require
 * a new permit + every {@code Backend.adapterFor} update.
 * <p>
 * See {@code .docs/storage-redesign/01-data-kinds.md}.
 *
 * @param <E> the mutable event type published on the write path
 * @param <R> the immutable record type read back through operations
 */
@ApiStatus.Experimental
public sealed interface DataKind<E, R>
        permits EventStream, Entity, KeyValueScoped, Counter {

    /** Local name within the declaring extension's namespace. */
    @NotNull String name();

    @NotNull Class<E> eventType();

    @NotNull Class<R> recordType();

    @NotNull Codec<R> codec();

    /** Capabilities the routed backend must advertise for registration to succeed. */
    @NotNull EnumSet<Capability> requiredCapabilities();

    /** Optional capabilities; missing ones produce a startup warning, not an error. */
    @NotNull EnumSet<Capability> optionalCapabilities();
}
