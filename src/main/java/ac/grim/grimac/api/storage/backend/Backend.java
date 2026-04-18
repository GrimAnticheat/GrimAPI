package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;

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

    String id();

    ApiVersion getApiVersion();

    EnumSet<Capability> capabilities();

    Set<Category<?>> supportedCategories();

    void init(BackendContext ctx) throws BackendException;

    void flush() throws BackendException;

    void close() throws BackendException;

    <R> void writeBatch(Category<R> cat, List<R> records) throws BackendException;

    <R> Page<R> read(Category<R> cat, Query<R> query) throws BackendException;

    <R> void delete(Category<R> cat, DeleteCriteria criteria) throws BackendException;

    /**
     * Count violations in a session. First-class (rather than via a generic count(query))
     * because phase 1 only needs this one aggregate and making it explicit is simpler than
     * a generic count query surface.
     */
    long countViolationsInSession(java.util.UUID sessionId) throws BackendException;
}
