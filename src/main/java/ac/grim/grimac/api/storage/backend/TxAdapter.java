package ac.grim.grimac.api.storage.backend;

import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.tx.TxBody;
import ac.grim.grimac.api.storage.tx.TxScope;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.EnumSet;

/**
 * Per-backend transaction SPI. Implementations execute the body atomically:
 * SQL via {@code Connection.autoCommit(false)}, Mongo via
 * {@code session.withTransaction}, Redis via Lua compilation.
 */
@ApiStatus.Experimental
public interface TxAdapter {

    @NotNull EnumSet<Capability> txCapabilities();

    /**
     * Run the body atomically. If {@code scope} is null, the implementation
     * must advertise {@code TX_GLOBAL}; otherwise it must advertise
     * {@code TX_LOCAL} (or {@code TX_GLOBAL}).
     */
    <T> T run(@Nullable TxScope scope, @NotNull TxBody<T> body) throws Exception;
}
