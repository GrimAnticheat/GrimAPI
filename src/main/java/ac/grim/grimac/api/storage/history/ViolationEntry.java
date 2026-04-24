package ac.grim.grimac.api.storage.history;

import ac.grim.grimac.api.storage.model.VerboseFormat;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One violation within a session detail, with its check's stable key and display
 * name already resolved. Offsets are relative to the owning session's start so Layer
 * 3 rendering is time-base-free.
 */
@ApiStatus.Experimental
public record ViolationEntry(
        int checkId,
        @NotNull String stableKey,
        @NotNull String displayName,
        @NotNull String description,
        long offsetFromSessionStartMs,
        double vl,
        @Nullable String verbose,
        @NotNull VerboseFormat verboseFormat) {

    public ViolationEntry {
        if (description == null) description = "";
    }
}
