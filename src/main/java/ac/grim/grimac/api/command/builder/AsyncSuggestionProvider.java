package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Async-friendly variant of {@link SuggestionProvider} for suggestion sources
 * that should not block the command thread (database lookups, network calls).
 * Bridged to Cloud's {@code SuggestionProvider} (non-blocking) internally.
 */
@FunctionalInterface
public interface AsyncSuggestionProvider {

    @NotNull CompletableFuture<? extends @NotNull List<@NotNull String>> suggestions(
            @NotNull GrimCommandContext context, @NotNull String input);
}
