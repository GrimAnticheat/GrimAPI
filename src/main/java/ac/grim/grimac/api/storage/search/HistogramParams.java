package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;

/**
 * Parameters for a {@link FacetType#HISTOGRAM} facet.
 *
 * @param min       inclusive lower bound of bucket 0
 * @param max       exclusive upper bound of the last bucket
 * @param buckets   number of equal-width buckets between min and max
 */
@ApiStatus.Experimental
public record HistogramParams(double min, double max, int buckets) {

    public HistogramParams {
        if (buckets <= 0) throw new IllegalArgumentException("buckets > 0");
        if (max <= min) throw new IllegalArgumentException("max > min");
    }
}
