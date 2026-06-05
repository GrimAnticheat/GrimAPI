package ac.grim.grimac.api.storage.kind;

import org.jetbrains.annotations.ApiStatus;

/**
 * Bucketing hint for {@link EventStream} stores. Backends with native
 * timeseries support (Mongo 5+) use this to size their internal buckets;
 * others ignore.
 */
@ApiStatus.Experimental
public enum Granularity {
    SECONDS,
    MINUTES,
    HOURS
}
