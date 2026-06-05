package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.DataKind;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.Optional;

/**
 * Narrow storage-engine SPI replacing the old {@link Backend}. Owns
 * connection lifecycle and capability advertisement; per-Kind work is
 * delegated to {@link KindAdapter}.
 * <p>
 * Coexists with the legacy {@link Backend} during the redesign window.
 * Phase 5 deletes the old SPI and renames this to {@code Backend}.
 */
@ApiStatus.Experimental
public interface BackendV2 {

    @NotNull String id();

    @NotNull ApiVersion apiVersion();

    @NotNull EnumSet<Capability> capabilities();

    void init(@NotNull BackendContext ctx) throws BackendException;

    void flush() throws BackendException;

    void close() throws BackendException;

    /**
     * Adapter for the given Kind, or empty if this backend does not host
     * that Kind. Called once per registered category at startup; the result
     * is cached by the routing layer.
     */
    <K extends DataKind<?, ?>> @NotNull Optional<KindAdapter<K>> adapterFor(@NotNull K kind);

    /**
     * Last-resort escape hatch. Returns the underlying client (e.g.
     * {@code MongoDatabase}, {@code java.sql.Connection}, {@code Jedis}) for
     * callers that have no portable alternative.
     */
    <X> @NotNull Optional<X> unwrap(@NotNull Class<X> type);

    /**
     * How many consumer threads the ring should allocate for writes to
     * the given category on this backend. The ring implementation
     * (Disruptor, Lattice, etc.) uses this to size its consumer pool.
     *
     * <p>Backends with physical single-writer constraints (SQLite)
     * return 1 for every category — hardcoded, not configurable.
     * Multi-writer backends (Postgres, MySQL, Mongo) read from their
     * per-backend config file under {@code writer-threads.default}
     * and {@code writer-threads.<category-id>} overrides.
     *
     * <p>Default is 1 (one consumer thread per category).
     */
    default int writerThreads(@NotNull Category<?> category) { return 1; }
}
