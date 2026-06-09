package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum MatchMode {
    /** Term match against the analyzed field. */
    TERM,
    /** Whole phrase. */
    PHRASE,
    /** Phrase with last token treated as prefix. */
    PHRASE_PREFIX,
    /** Levenshtein fuzzy. Requires {@code SEARCH_FUZZY}. */
    FUZZY,
    /** Glob/wildcard. */
    WILDCARD
}
