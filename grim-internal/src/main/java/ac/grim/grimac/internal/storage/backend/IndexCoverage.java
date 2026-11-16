package ac.grim.grimac.internal.storage.backend;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Locale;

/**
 * Decides whether an index Grim would create is already served by one the
 * table has. Backends read their indexes into {@link Existing} records; this
 * class only compares them, so the rule is the same on every database.
 *
 * <p>An existing index covers a required non-unique index when its leading key
 * columns are the required columns, in the same order. With more than one
 * required column the sort directions must all match, or all be reversed (the
 * index is then scanned backwards). A required unique index is covered only by
 * a unique index on exactly the same columns, since a unique index on more
 * columns does not enforce uniqueness of its prefix.
 *
 * <p>Backends mark an index unusable when the planner cannot use it for every
 * row or ordering a default index serves: partial, sparse, hash, expression,
 * column-prefix, non-default collation, or invisible indexes.
 */
@ApiStatus.Internal
public final class IndexCoverage {

    private IndexCoverage() {
    }

    /** One key column; {@code name} is null for an expression. */
    public record Column(@Nullable String name, boolean descending) {
    }

    /** An index the table already has. */
    public record Existing(@NotNull String name, boolean unique, boolean usable, @NotNull List<Column> keys) {
        public Existing {
            keys = List.copyOf(keys);
        }
    }

    /**
     * @param required index fields as Grim declares them: a leading {@code -} marks a descending column
     * @param unique   whether the required index is unique
     */
    public static boolean covers(@NotNull Existing index, @NotNull List<String> required, boolean unique) {
        if (!index.usable() || required.isEmpty() || index.keys().size() < required.size()) return false;
        if (unique && (!index.unique() || index.keys().size() != required.size())) return false;
        boolean sameDirection = true;
        boolean reversed = true;
        for (int i = 0; i < required.size(); i++) {
            String field = required.get(i);
            boolean descending = field.startsWith("-");
            String name = descending ? field.substring(1) : field;
            Column column = index.keys().get(i);
            if (column.name() == null
                    || !column.name().toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
                return false;
            }
            sameDirection &= column.descending() == descending;
            reversed &= column.descending() != descending;
        }
        // A single column is read in either direction; uniqueness does not depend on direction.
        return unique || required.size() == 1 || sameDirection || reversed;
    }
}
