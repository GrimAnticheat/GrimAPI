package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.List;

@ApiStatus.Experimental
public record Page<R>(List<R> items, @Nullable Cursor nextCursor) {

    public Page {
        items = items == null ? List.of() : List.copyOf(items);
    }

    public static <R> Page<R> empty() {
        return new Page<>(List.of(), null);
    }

    public boolean hasMore() {
        return nextCursor != null;
    }
}
