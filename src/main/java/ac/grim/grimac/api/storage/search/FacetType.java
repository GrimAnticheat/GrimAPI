package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum FacetType {
    /** Group by distinct term values, top-N. */
    TERMS,
    /** Numeric histogram with fixed bucket width. */
    HISTOGRAM,
    /** Date histogram with calendar-aware bucketing. */
    DATE_HISTOGRAM,
    /** Pre-declared numeric ranges. */
    RANGE
}
