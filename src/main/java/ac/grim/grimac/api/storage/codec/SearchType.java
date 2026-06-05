package ac.grim.grimac.api.storage.codec;

import org.jetbrains.annotations.ApiStatus;

/**
 * Kind of search index to create for a {@link Searchable} field.
 * Backends advertise per-type capability via their search adapter.
 */
@ApiStatus.Experimental
public enum SearchType {
    /** Exact-equality match / tag. */
    KEYWORD,
    /** Prefix match (e.g. player-name autocomplete). */
    KEYWORD_PREFIX,
    /** Tokenized full-text. */
    TEXT,
    /** Range-queryable numeric. */
    NUMERIC,
    /** Similarity search; requires dimension on {@link Searchable#dimension()}. */
    VECTOR
}
