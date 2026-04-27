package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Concrete {@link GrimCommand.Built} carrying the captured {@link GrimCommand.Spec}.
 *
 * <p>Public so the internal Cloud bridge in the common module can read the
 * spec, but tagged {@code @ApiStatus.Internal} — extensions should treat
 * {@link GrimCommand.Built} as opaque.
 */
@ApiStatus.Internal
public final class BuiltImpl implements GrimCommand.Built {

    private final GrimCommand.Spec spec;

    public BuiltImpl(@NotNull GrimCommand.Spec spec) {
        this.spec = spec;
    }

    public @NotNull GrimCommand.Spec spec() {
        return spec;
    }
}
