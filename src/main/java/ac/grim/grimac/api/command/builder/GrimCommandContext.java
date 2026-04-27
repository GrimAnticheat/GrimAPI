package ac.grim.grimac.api.command.builder;

import ac.grim.grimac.api.command.CommandSender;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * View over a command invocation. Wraps Cloud's {@code CommandContext}; the
 * Cloud type stays internal.
 */
public interface GrimCommandContext {

    @NotNull CommandSender sender();

    /**
     * Returns the value of a required argument. Throws if the argument is
     * missing or the type is wrong.
     */
    <T> @NotNull T get(@NotNull String key);

    /**
     * Returns the value of an optional argument, if present.
     */
    <T> @NotNull Optional<T> optional(@NotNull String key);

    /**
     * @return true if the named flag was present on the command line
     */
    boolean flag(@NotNull String name);

    /**
     * Returns the value of a value-flag, if present.
     */
    <T> @NotNull Optional<T> flagValue(@NotNull String name);
}
