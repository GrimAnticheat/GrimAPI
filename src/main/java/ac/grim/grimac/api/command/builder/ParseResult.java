package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Result of {@link ArgumentParser#parse(GrimCommandContext, GrimCommandInput)}.
 * Either holds a value (success) or an error message (failure).
 */
public final class ParseResult<T> {

    private final T value;
    private final String error;

    private ParseResult(@Nullable T value, @Nullable String error) {
        this.value = value;
        this.error = error;
    }

    public static <T> @NotNull ParseResult<T> ok(@NotNull T value) {
        return new ParseResult<>(value, null);
    }

    public static <T> @NotNull ParseResult<T> fail(@NotNull String error) {
        return new ParseResult<>(null, error);
    }

    public boolean isOk() {
        return error == null;
    }

    public @Nullable T value() {
        return value;
    }

    public @Nullable String error() {
        return error;
    }
}
