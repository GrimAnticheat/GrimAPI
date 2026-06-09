package ac.grim.grimac.api.storage.admin;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;

@ApiStatus.Experimental
public record IndexStats(@NotNull List<Entry> entries) {

    public IndexStats {
        entries = List.copyOf(entries);
    }

    public record Entry(
            @NotNull String name,
            long sizeBytes,
            long entryCount,
            double usageRate) {}
}
