package ac.grim.grimac.api.storage.kind;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable read-side record returned by {@link KeyValueScoped} get
 * operations. Carries the scope tuple plus the looked-up value.
 */
@ApiStatus.Experimental
public record KeyValueRecord<S, V>(
        @NotNull S scope,
        @NotNull String scopeKey,
        @NotNull String key,
        @NotNull V value,
        long updatedEpochMs) {
}
