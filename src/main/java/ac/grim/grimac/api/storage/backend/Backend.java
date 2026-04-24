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
 * <strong>Write path</strong>: the DataStore wires a ring buffer per category
 * and feeds each ring's consumer through the {@link StorageEventHandler}
 * returned by {@link #eventHandlerFor(Category)}. Slots are pre-allocated
 * mutable events and {@code endOfBatch} signals commit boundaries. Handlers
 * for sibling categories on the same backend instance may run concurrently;
 * implementations must synchronise any shared write state internally.
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
     * objects (not events) for the given category, bypassing the ring. Used by
     * startup-time importers that need the records visible before
     * {@link #eventHandlerFor} starts accepting live traffic. The two importers
     * that drive this today:
     * <ul>
     *   <li><strong>V0 → V1 history migration</strong> — on first boot after
     *       upgrading from a pre-v1 Grim, rows from the legacy
     *       {@code violations.sqlite} are replayed into the routed backend for
     *       the violation category so {@code /grim history} is continuous
     *       across the upgrade. See {@link ac.grim.grimac.api.storage.config.MigrationConfig}.</li>
     *   <li><strong>Cross-backend copy</strong> — {@code /grim history copy}
     *       moves an operator from one backend to another (e.g. SQLite to
     *       MySQL) without losing history.</li>
     * </ul>
     * <p>
     * Callers must pass records whose runtime type matches
     * {@code cat.queryResultType()}; implementations cast internally and throw
     * on mismatch. Synchronous, blocks until committed. Not intended for the
     * hot path — live writes go through the ring.
     * <p>
     * A backend that wants to be a migration target must implement this; the
     * default throws {@link UnsupportedOperationException} so read-only-ish
     * backends fail loudly rather than silently swallowing imported data.
     */
    default <R> void bulkImport(@NotNull Category<?> cat, @NotNull List<R> records) throws BackendException {
        throw new UnsupportedOperationException(
                "backend " + id() + " does not support bulkImport; cannot be used as a V0 → V1 migration or cross-backend copy target");
    }

    /**
     * Count violations in a session. First-class (rather than via a generic
     * {@code count(query)}) because the current feature set only needs this
     * one aggregate and an explicit method is simpler than a generic count
     * query surface.
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
