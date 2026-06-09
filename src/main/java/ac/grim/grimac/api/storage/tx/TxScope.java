package ac.grim.grimac.api.storage.tx;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Co-location scope for a {@link Transaction}. Drives Redis hash-tag
 * generation so all keys touched in the transaction hash to the same Redis
 * Cluster slot. Ignored on SQL and Mongo, where transactions are slot-free.
 * <p>
 * Backends that advertise only {@code TX_LOCAL} require every transaction
 * to declare a scope; only backends advertising {@code TX_GLOBAL} support
 * scope-free transactions.
 */
@ApiStatus.Experimental
public sealed interface TxScope {

    /** Hash-tag string injected into Redis keys. */
    @NotNull String tag();

    record Player(@NotNull UUID uuid) implements TxScope {
        @Override public @NotNull String tag() { return "player:" + uuid; }
    }

    record Server(@NotNull String name) implements TxScope {
        @Override public @NotNull String tag() { return "server:" + name; }
    }

    record Custom(@NotNull String value) implements TxScope {
        @Override public @NotNull String tag() { return value; }
    }
}
