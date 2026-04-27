package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.NotNull;

/**
 * View over the unconsumed tail of a command's input. Parsers consume tokens
 * via {@link #readString()} and may inspect upcoming tokens with
 * {@link #peekString()}.
 *
 * <p>Bridged from Cloud's {@code CommandInput}; extensions never see the Cloud
 * type.
 */
public interface GrimCommandInput {

    /**
     * Returns the next token without consuming it. Returns the empty string
     * when there is no more input.
     */
    @NotNull String peekString();

    /**
     * Consumes and returns the next token. Returns the empty string when there
     * is no more input.
     */
    @NotNull String readString();

    /**
     * Returns the entire unconsumed remainder as a single space-joined string.
     * Does not consume.
     */
    @NotNull String remaining();

    /**
     * @return true if no further tokens are available
     */
    boolean isEmpty();
}
