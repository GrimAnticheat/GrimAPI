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
 * Storage engine SPI. Implementations map {@link Category} records to native storage.
 * <p>
 * Implementors declare an {@link ApiVersion} (the Java contract they were compiled against)
 * and an {@link EnumSet} of {@link Capability} they provide; the shared facade validates
 * category routing against these at startup.
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

    <R> void writeBatch(@NotNull Category<R> cat, @NotNull List<R> records) throws BackendException;

    @NotNull <R> Page<R> read(@NotNull Category<R> cat, @NotNull Query<R> query) throws BackendException;

    <R> void delete(@NotNull Category<R> cat, @NotNull DeleteCriteria criteria) throws BackendException;

    /**
     * Count violations in a session. First-class (rather than via a generic count(query))
     * because phase 1 only needs this one aggregate and making it explicit is simpler than
     * a generic count query surface.
     */
    long countViolationsInSession(@NotNull java.util.UUID sessionId) throws BackendException;
}
