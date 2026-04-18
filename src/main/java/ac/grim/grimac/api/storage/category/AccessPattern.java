package ac.grim.grimac.api.storage.category;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum AccessPattern {
    INDEXED_KV,
    TIMESERIES,
    BLOB_REF,
    KV
}
