package ac.grim.grimac.api.storage.search;

import ac.grim.grimac.api.storage.query.Cursor;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Portable search query AST. Compiled to each backend's native FTS / vector
 * primitives by its {@code SearchAdapter}. See
 * {@code .docs/storage-redesign/04-search-tx-admin.md}.
 */
@ApiStatus.Experimental
public record SearchSpec(
        @NotNull List<Match> matches,
        @NotNull List<Filter> filters,
        @NotNull List<FacetRequest> facets,
        @NotNull List<Sort> sort,
        @Nullable VectorQuery vector,
        @NotNull Rank rank,
        int pageSize,
        @Nullable Cursor cursor) {

    public SearchSpec {
        matches = List.copyOf(matches);
        filters = List.copyOf(filters);
        facets = List.copyOf(facets);
        sort = List.copyOf(sort);
    }

    public record Match(@NotNull String field, @NotNull String query, @NotNull MatchMode mode, double boost) {}

    public record Filter(@NotNull String field, @NotNull FilterOp op, @Nullable Object value) {}

    public record FacetRequest(@NotNull String field, @NotNull FacetType type, @Nullable Object params) {}

    public record Sort(@NotNull String field, @NotNull Direction direction) {}

    public record VectorQuery(@NotNull String field, float @NotNull [] vector, int k) {}

    public static @NotNull Builder builder() { return new Builder(); }

    public static final class Builder {
        private final List<Match> matches = new ArrayList<>();
        private final List<Filter> filters = new ArrayList<>();
        private final List<FacetRequest> facets = new ArrayList<>();
        private final List<Sort> sort = new ArrayList<>();
        private VectorQuery vector;
        private Rank rank = Rank.BM25;
        private int pageSize = 20;
        private Cursor cursor;

        public @NotNull Builder match(@NotNull String field, @NotNull String query, @NotNull MatchMode mode) {
            return match(field, query, mode, 1.0);
        }
        public @NotNull Builder match(@NotNull String field, @NotNull String query, @NotNull MatchMode mode, double boost) {
            matches.add(new Match(field, query, mode, boost)); return this;
        }
        public @NotNull Builder filter(@NotNull String field, @NotNull FilterOp op, @Nullable Object value) {
            filters.add(new Filter(field, op, value)); return this;
        }
        public @NotNull Builder facet(@NotNull String field, @NotNull FacetType type) {
            facets.add(new FacetRequest(field, type, null)); return this;
        }
        public @NotNull Builder facet(@NotNull String field, @NotNull FacetType type, @NotNull Object params) {
            facets.add(new FacetRequest(field, type, params)); return this;
        }
        public @NotNull Builder sort(@NotNull String field, @NotNull Direction dir) {
            sort.add(new Sort(field, dir)); return this;
        }
        public @NotNull Builder vector(@NotNull String field, float @NotNull [] v, int k) {
            this.vector = new VectorQuery(field, v, k); return this;
        }
        public @NotNull Builder rank(@NotNull Rank r) { this.rank = r; return this; }
        public @NotNull Builder pageSize(int n) { this.pageSize = n; return this; }
        public @NotNull Builder cursor(@Nullable Cursor c) { this.cursor = c; return this; }

        public @NotNull SearchSpec build() {
            return new SearchSpec(matches, filters, facets, sort, vector, rank, pageSize, cursor);
        }
    }
}
