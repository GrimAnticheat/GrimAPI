package ac.grim.grimac.api.storage.check;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

@ApiStatus.Experimental
public record CheckCatalogRow(
        int checkId,
        String stableKey,
        @Nullable String display,
        @Nullable String description,
        @Nullable String introducedVersion,
        long introducedAt) {
}
