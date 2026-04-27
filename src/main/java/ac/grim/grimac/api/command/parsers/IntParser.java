package ac.grim.grimac.api.command.parsers;

import ac.grim.grimac.api.command.builder.ArgumentParser;
import ac.grim.grimac.api.command.builder.GrimCommandContext;
import ac.grim.grimac.api.command.builder.GrimCommandInput;
import ac.grim.grimac.api.command.builder.ParseResult;
import org.jetbrains.annotations.NotNull;

public final class IntParser {

    private IntParser() {
    }

    public static @NotNull ArgumentParser<Integer> integer() {
        return new Impl(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static @NotNull ArgumentParser<Integer> between(int min, int max) {
        return new Impl(min, max);
    }

    private record Impl(int min, int max) implements ArgumentParser<Integer> {
        @Override
        public @NotNull ParseResult<Integer> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            String token = input.readString();
            if (token.isEmpty()) return ParseResult.fail("Expected an integer");
            try {
                int v = Integer.parseInt(token);
                if (v < min || v > max) {
                    return ParseResult.fail("Integer out of range [" + min + ", " + max + "]: " + v);
                }
                return ParseResult.ok(v);
            } catch (NumberFormatException e) {
                return ParseResult.fail("Not an integer: " + token);
            }
        }

        @Override
        public @NotNull Class<Integer> valueType() {
            return Integer.class;
        }
    }
}
