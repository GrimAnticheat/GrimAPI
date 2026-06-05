package ac.grim.grimac.api.storage.kind;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Declares a secondary index on an {@link Entity} store. Backends create
 * the index at {@code ensureStore} time.
 * <p>
 * Field names may carry a leading {@code -} to indicate descending order
 * (e.g. {@code "-started_at"}); the parser splits the leading sign off.
 */
@ApiStatus.Experimental
public record IndexSpec(
        @NotNull String name,
        @NotNull List<String> fields,
        boolean unique,
        boolean caseInsensitivePrefix) {

    public IndexSpec {
        fields = List.copyOf(fields);
        if (name.isEmpty()) throw new IllegalArgumentException("name");
        if (fields.isEmpty()) throw new IllegalArgumentException("fields");
    }

    public static @NotNull IndexSpec of(@NotNull String name, @NotNull String... fields) {
        return new IndexSpec(name, List.of(fields), false, false);
    }

    public static @NotNull IndexSpec unique(@NotNull String name, @NotNull String... fields) {
        return new IndexSpec(name, List.of(fields), true, false);
    }
}
