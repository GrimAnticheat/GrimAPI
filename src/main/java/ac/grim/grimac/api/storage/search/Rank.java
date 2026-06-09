package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum Rank {
    /** BM25 (Lucene-family default). */
    BM25,
    /** Classic TF-IDF (Postgres ts_rank, MySQL default). */
    TFIDF,
    /** Backend's default native scoring. */
    NATIVE,
    /** No relevance scoring; ordered by sort or insertion. */
    NONE
}
