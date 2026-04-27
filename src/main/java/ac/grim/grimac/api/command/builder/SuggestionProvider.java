package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Returns tab-completion suggestions for a partial input.
 *
 * <p>Bridged to Cloud's {@code BlockingSuggestionProvider} internally — the
 * call runs synchronously on the platform's command thread. For long-running
 * suggestion sources, use {@link AsyncSuggestionProvider} instead.
 */
@FunctionalInterface
public interface SuggestionProvider {

    @NotNull List<String> suggestions(@NotNull GrimCommandContext context, @NotNull String input);
}
