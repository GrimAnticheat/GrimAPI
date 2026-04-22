package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Storage engine SPI. Implementations map {@link Category} events to native storage.
 * <p>
 * Implementors declare an {@link ApiVersion} (the Java contract they were compiled
 * against) and an {@link EnumSet} of {@link Capability} they provide; the shared
 * facade validates category routing against these at startup.
 * <p>
 * <strong>Write path</strong>: Layer 2 wires a Disruptor ring buffer per category;
 * slots are pre-allocated mutable events. The ring's consumer invokes the
 * {@link StorageEventHandler} returned from {@link #eventHandlerFor(Category)}
 * in sequence, with {@code endOfBatch} signalling commit boundaries. Handlers
 * may run concurrently with other handlers for sibling categories on the same
 * backend instance; implementations must synchronise shared write state
 * internally.
 * <p>
 * <strong>Read path</strong>: {@link #read(Category, Query)} is on-thread; the
 * {@code Category} argument is the routing key, while the result type comes from
 * the {@link Query}.
 */
@ApiStatus.Experimental
public interface Backend {

    @NotNull String id();

    @NotNull ApiVersion getApiVersion();

    @NotNull EnumSet<Capability> capabilities();

    @NotNull Set<Category<?>> supportedCategories();

    void init(@NotNull BackendContext ctx) throws BackendException;

    void flush() throws BackendException;

    void close() throws BackendException;

    /**
     * Provide a write consumer for the given category's ring. Called once per
     * category at DataStore startup (wiring time), not on the hot path. Returned
     * handlers own their batching state.
     */
    @NotNull <E> StorageEventHandler<E> eventHandlerFor(@NotNull Category<E> cat) throws BackendException;

    /**
     * Synchronous read. The {@code cat} argument is the routing key — the result
     * record type comes from {@code query}. Implementations should assert at
     * runtime that {@code query} targets a type consistent with
     * {@code cat.queryResultType()}.
     */
    @NotNull <R> Page<R> read(@NotNull Category<?> cat, @NotNull Query<R> query) throws BackendException;

    <E> void delete(@NotNull Category<E> cat, @NotNull DeleteCriteria criteria) throws BackendException;

    /**
     * Synchronous bulk-import escape hatch. Writes a batch of immutable record
     * objects (not events) for the given category outside the Disruptor ring.
     * Used by startup-time importers (e.g. legacy v0 → v1 migration) that
     * require the records to be visible before {@link #eventHandlerFor} begins
     * accepting live traffic.
     * <p>
     * Callers must pass records whose runtime type matches
     * {@code cat.queryResultType()}; implementations cast internally and throw
     * on mismatch. Synchronous, blocks until committed. Not intended for the
     * hot path — live writes go through the ring.
     * <p>
     * Implementations added after phase 1 that want to support being a
     * migration target must implement this; the default throws
     * {@link UnsupportedOperationException} so read-only-ish backends fail
     * clearly.
     */
    default <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        throw new UnsupportedOperationException(
                "backend " + id() + " does not support bulkImport; cannot be used as a migration target");
    }

    /**
     * Count violations in a session. First-class (rather than via a generic count(query))
     * because phase 1 only needs this one aggregate and making it explicit is simpler than
     * a generic count query surface.
     */
    long countViolationsInSession(@NotNull java.util.UUID sessionId) throws BackendException;

    /**
     * Count distinct checks that flagged in a given session. Feeds the
     * {@code [N]} unique-check-count indicator on session-list lines.
     * Default throws {@link UnsupportedOperationException}; the session/history
     * commands degrade gracefully when a backend doesn't implement it.
     */
    default long countUniqueChecksInSession(@NotNull java.util.UUID sessionId) throws BackendException {
        throw new UnsupportedOperationException(
                "backend " + id() + " does not support countUniqueChecksInSession");
    }

    /**
     * Count sessions recorded for a player. Feeds the {@code [page / maxPages]}
     * label on the session-list header. Default throws; see
     * {@link #countUniqueChecksInSession} for the rationale.
     */
    default long countSessionsByPlayer(@NotNull java.util.UUID player) throws BackendException {
        throw new UnsupportedOperationException(
                "backend " + id() + " does not support countSessionsByPlayer");
    }
}
