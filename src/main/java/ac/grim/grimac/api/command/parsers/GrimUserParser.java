package ac.grim.grimac.api.command.parsers;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.command.builder.ArgumentParser;
import ac.grim.grimac.api.command.builder.GrimCommandContext;
import ac.grim.grimac.api.command.builder.GrimCommandInput;
import ac.grim.grimac.api.command.builder.ParseResult;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Platform-neutral parser for {@link GrimUser}. The actual lookup
 * implementation is set by GrimAC's loader at platform startup
 * (Bukkit/Fabric/Velocity each install their own); extensions just call
 * {@link #parser()} and get a parser that works on every platform.
 *
 * <p>Suggestions list online players via the same platform-specific source.
 */
public final class GrimUserParser {

    private GrimUserParser() {
    }

    /**
     * Returns the platform-neutral GrimUser parser. Throws if the
     * platform-specific implementation has not yet been installed (i.e., if
     * called before GrimAC has reached the start phase).
     */
    public static @NotNull ArgumentParser<GrimUser> parser() {
        ArgumentParser<GrimUser> impl = IMPL.get();
        if (impl == null) {
            throw new IllegalStateException("GrimUserParser is not yet available. " +
                    "Wait until GrimAC has reached the start phase before registering commands.");
        }
        return impl;
    }

    /**
     * Installs the per-platform implementation. Called by GrimAC's loader at
     * startup; extensions must not call this.
     */
    @ApiStatus.Internal
    public static void install(@NotNull ArgumentParser<GrimUser> impl) {
        IMPL.set(impl);
    }

    private static final AtomicReference<ArgumentParser<GrimUser>> IMPL = new AtomicReference<>();

    /**
     * Default fallback used when no platform-specific impl is installed yet:
     * fails parse with a clear message, returns no suggestions.
     */
    @ApiStatus.Internal
    public static @NotNull ArgumentParser<GrimUser> defaultUnavailable() {
        return UNAVAILABLE;
    }

    private static final ArgumentParser<GrimUser> UNAVAILABLE = new ArgumentParser<>() {
        @Override
        public @NotNull ParseResult<GrimUser> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            return ParseResult.fail("GrimUser parser is not available on this platform yet.");
        }

        @Override
        public @NotNull Class<GrimUser> valueType() {
            return GrimUser.class;
        }
    };
}
