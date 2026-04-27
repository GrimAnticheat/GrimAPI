package ac.grim.grimac.api.command.parsers;

import ac.grim.grimac.api.command.builder.ArgumentParser;
import ac.grim.grimac.api.command.builder.GrimCommandContext;
import ac.grim.grimac.api.command.builder.GrimCommandInput;
import ac.grim.grimac.api.command.builder.ParseResult;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public final class UUIDParser {

    private UUIDParser() {
    }

    public static @NotNull ArgumentParser<UUID> parser() {
        return Impl.INSTANCE;
    }

    private static final class Impl implements ArgumentParser<UUID> {
        static final Impl INSTANCE = new Impl();

        @Override
        public @NotNull ParseResult<UUID> parse(@NotNull GrimCommandContext context, @NotNull GrimCommandInput input) {
            String token = input.readString();
            if (token.isEmpty()) return ParseResult.fail("Expected a UUID");
            try {
                return ParseResult.ok(UUID.fromString(token));
            } catch (IllegalArgumentException e) {
                return ParseResult.fail("Not a valid UUID: " + token);
            }
        }

        @Override
        public @NotNull Class<UUID> valueType() {
            return UUID.class;
        }
    }
}
