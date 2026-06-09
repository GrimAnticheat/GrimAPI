package ac.grim.grimac.api.storage.search;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Inclusive range value for {@link FilterOp#RANGE} filters. Either bound may
 * be null for half-open ranges.
 */
@ApiStatus.Experimental
public record Range(@Nullable Object from, @Nullable Object to) {

    public static @NotNull Range of(@Nullable Object from, @Nullable Object to) {
        return new Range(from, to);
    }

    public static @NotNull Range atLeast(@NotNull Object from) { return new Range(from, null); }
    public static @NotNull Range atMost(@NotNull Object to)    { return new Range(null, to); }
}
