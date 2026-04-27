package ac.grim.grimac.api.storage.category;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum Capability {
    INDEXED_KV,
    TIMESERIES_APPEND,
    BLOB,
    TTL,
    TRANSACTIONS,
    ATOMIC_COUNTER,

    HISTORY,
    SETTINGS,
    REPLAY,
    PLAYER_IDENTITY
}
