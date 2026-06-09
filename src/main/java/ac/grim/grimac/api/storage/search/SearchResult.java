package ac.grim.grimac.api.storage.search;

import ac.grim.grimac.api.storage.query.Cursor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Result of a {@link SearchSpec} execution. Hits carry both the
 * deserialized record and its score; facets are keyed by request field;
 * {@code nextCursor} is non-null when more pages exist.
 */
@ApiStatus.Experimental
public record SearchResult<R>(
        @NotNull List<Scored<R>> hits,
        @NotNull Map<String, FacetResult> facets,
        long totalEstimate,
        @Nullable Cursor nextCursor) {

    public SearchResult {
        hits = List.copyOf(hits);
        facets = Map.copyOf(facets);
    }

    public boolean hasMore() { return nextCursor != null; }
}
