package ac.grim.grimac.api.storage.tx;

import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Inside-transaction handle passed to {@link TxBody#run}. All operations
 * submitted or executed through this handle are part of the same atomic unit;
 * SQL/Mongo wrap them in a real transaction, Redis compiles them to one Lua
 * script.
 */
@ApiStatus.Experimental
public interface Transaction {

    <E> void submit(@NotNull Category<E> cat, @NotNull Consumer<E> configurer);

    <R> R execute(@NotNull Operation<R> op) throws Exception;

    /** Explicit rollback. Equivalent to throwing from the body. */
    void rollback();
}
