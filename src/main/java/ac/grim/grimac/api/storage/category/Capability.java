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
    ATOMIC_UPSERT,
    BINARY_UUID_KEYS,
    MULTI_WRITER,

    HISTORY,
    SETTINGS,
    REPLAY,
    PLAYER_IDENTITY,
    EXTENSION_STORAGE,

    KIND_ENTITY,
    KIND_EVENT_STREAM,
    KIND_KV_SCOPED,
    KIND_COUNTER,

    EVENT_STREAM_TIMESERIES_NATIVE,
    EVENT_STREAM_TTL_NATIVE,
    EVENT_STREAM_RANGE_BY_TIME
}
