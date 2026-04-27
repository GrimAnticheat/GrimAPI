package ac.grim.grimac.api.command.parsers;

import ac.grim.grimac.api.command.builder.ArgumentParser;
import ac.grim.grimac.api.command.builder.GrimCommandContext;
import ac.grim.grimac.api.command.builder.GrimCommandInput;
import ac.grim.grimac.api.command.builder.ParseResult;
import org.jetbrains.annotations.NotNull;

public final class StringParser {

    private StringParser() {
    }

    /** Consumes a single space-delimited token. */
    public static @NotNull ArgumentParser<String> word() {
        return Word.INSTANCE;
    }

    /** Consumes the entire remainder of the command input as one string. */
    public static @NotNull ArgumentParser<String> greedy() {
        return Greedy.INSTANCE;
    }

    private static final class Word implements ArgumentParser<String> {
        static final Word INSTANCE = new Word();

        @Override
        public @NotNull ParseResult<String> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            String token = input.readString();
            if (token.isEmpty()) return ParseResult.fail("Expected a word");
            return ParseResult.ok(token);
        }

        @Override
        public @NotNull Class<String> valueType() {
            return String.class;
        }
    }

    private static final class Greedy implements ArgumentParser<String> {
        static final Greedy INSTANCE = new Greedy();

        @Override
        public @NotNull ParseResult<String> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            String remainder = input.remaining();
            if (remainder.isEmpty()) return ParseResult.fail("Expected a string");
            // Drain the input — reading until empty mirrors the greedy contract.
            while (!input.isEmpty()) input.readString();
            return ParseResult.ok(remainder);
        }

        @Override
        public @NotNull Class<String> valueType() {
            return String.class;
        }
    }
}
