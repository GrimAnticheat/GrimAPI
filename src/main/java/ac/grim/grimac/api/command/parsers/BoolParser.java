package ac.grim.grimac.api.command.parsers;

import ac.grim.grimac.api.command.builder.ArgumentParser;
import ac.grim.grimac.api.command.builder.GrimCommandContext;
import ac.grim.grimac.api.command.builder.GrimCommandInput;
import ac.grim.grimac.api.command.builder.ParseResult;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;

public final class BoolParser {

    private BoolParser() {
    }

    public static @NotNull ArgumentParser<Boolean> parser() {
        return Impl.INSTANCE;
    }

    private static final class Impl implements ArgumentParser<Boolean> {
        static final Impl INSTANCE = new Impl();
        private static final List<String> SUGGESTIONS = Arrays.asList("true", "false");

        @Override
        public @NotNull ParseResult<Boolean> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            String token = input.readString();
            if (token.equalsIgnoreCase("true") || token.equalsIgnoreCase("yes") || token.equalsIgnoreCase("on")) {
                return ParseResult.ok(Boolean.TRUE);
            }
            if (token.equalsIgnoreCase("false") || token.equalsIgnoreCase("no") || token.equalsIgnoreCase("off")) {
                return ParseResult.ok(Boolean.FALSE);
            }
            return ParseResult.fail("Not a boolean: " + token);
        }

        @Override
        public @NotNull List<String> suggestions(@NotNull GrimCommandContext context, @NotNull String input) {
            return SUGGESTIONS;
        }

        @Override
        public @NotNull Class<Boolean> valueType() {
            return Boolean.class;
        }
    }
}
