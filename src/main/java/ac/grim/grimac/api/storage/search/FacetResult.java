package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Faceting result for one declared {@link FacetType}. Buckets carry the
 * facet value (term, histogram-bin midpoint, range label) and the document
 * count; histogram facets additionally expose lower / upper edges.
 */
@ApiStatus.Experimental
public record FacetResult(@NotNull FacetType type, @NotNull List<Bucket> buckets) {

    public FacetResult {
        buckets = List.copyOf(buckets);
    }

    public record Bucket(
            @NotNull Object value,
            long count,
            @Nullable Double lowerBound,
            @Nullable Double upperBound) {}
}
