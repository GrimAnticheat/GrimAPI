package ac.grim.grimac.api.storage.category;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.function.Supplier;

/**
 * Identity for a class of records flowing through the data store. The generic
 * parameter {@code E} names the mutable <em>event</em> type published on the
 * Disruptor write path; the corresponding immutable read-side DTO is declared
 * separately via {@link #queryResultType()} and flows through {@link
 * ac.grim.grimac.api.storage.query.Query} and {@link
 * ac.grim.grimac.api.storage.query.Page}.
 * <p>
 * Keeping write-event types and read-record types on separate type parameters —
 * Category on {@code E}, Query on its own {@code R} — is what lets the write
 * path be allocation-free (pre-allocated mutable events in the ring) while the
 * read path stays value-semantic (immutable records that survive across calls).
 */
@ApiStatus.Experimental
public interface Category<E> {

    @NotNull String id();

    /**
     * Concrete event class stored in the ring for this category. Must match
     * the type produced by {@link #newEvent()}.
     */
    @NotNull Class<E> eventType();

    /**
     * Factory for pre-allocating ring slots. Called once per slot at startup,
     * never on the hot path.
     */
    @NotNull Supplier<E> newEvent();

    /**
     * Immutable read-side DTO type. Returned from
     * {@link ac.grim.grimac.api.storage.backend.Backend#read}; backends
     * materialise a fresh instance from storage, never by copying an event.
     */
    @NotNull Class<?> queryResultType();

    @NotNull EnumSet<Capability> requiredCapabilities();

    @NotNull AccessPattern accessPattern();
}
