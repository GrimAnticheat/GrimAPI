package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * Parses a token (or tokens) from a command input into a typed value.
 * Implementations should consume tokens via
 * {@link GrimCommandInput#readString()} and return either
 * {@link ParseResult#ok(Object)} on success or
 * {@link ParseResult#fail(String)} on failure.
 *
 * <p>Override {@link #suggestions(GrimCommandContext, String)} to provide tab
 * completions; the default returns an empty list.
 *
 * <p>Implementations live in extension code or in the api-public
 * {@code parsers/} package and are bridged into Cloud parsers internally —
 * extensions never see the Cloud type.
 *
 * @param <T> the type produced by this parser
 */
public interface ArgumentParser<T> {

    /**
     * Parses tokens from {@code input} into a value.
     */
    @NotNull ParseResult<T> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input);

    /**
     * Returns suggestions for the current partial input. Default: empty list.
     */
    default @NotNull List<String> suggestions(@NotNull GrimCommandContext context, @NotNull String input) {
        return Collections.emptyList();
    }

    /**
     * Returns the produced value's class. Used by the bridge to register the
     * parser with Cloud's type-keyed parser registry.
     */
    @NotNull Class<T> valueType();
}
