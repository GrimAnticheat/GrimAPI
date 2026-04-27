package ac.grim.grimac.api.command.builder;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Entry point for the Cloud-shaped command builder facade. Extensions construct
 * commands via {@link #builder(String, String...)} and register them through
 * {@link ac.grim.grimac.api.command.CommandRegistry#registerBuilt}.
 *
 * <p>The builder mirrors Cloud's command builder by shape only — extension
 * code never imports {@code org.incendo.cloud.*}. Internally a bridge
 * translates the recorded {@link Spec} into Cloud calls at registration time.
 */
public final class GrimCommand {

    private GrimCommand() {
    }

    /**
     * Creates a new builder for a top-level command. The first name is the
     * primary command literal; remaining names are aliases.
     */
    public static @NotNull Builder builder(@NotNull String name, @NotNull String... aliases) {
        return new BuilderImpl(name, aliases);
    }

    public interface Builder {
        @NotNull Builder literal(@NotNull String name, @NotNull String... aliases);

        <T> @NotNull Builder required(@NotNull String name, @NotNull ArgumentParser<T> parser);

        <T> @NotNull Builder optional(@NotNull String name, @NotNull ArgumentParser<T> parser);

        /**
         * Adds a presence flag — present if the user typed {@code --name} or any alias.
         */
        @NotNull Builder flag(@NotNull String name, @NotNull String... aliases);

        /**
         * Adds a value flag — {@code --name <value>} parsed by the given parser.
         */
        <T> @NotNull Builder valueFlag(@NotNull String name, @NotNull ArgumentParser<T> parser, @NotNull String... aliases);

        @NotNull Builder permission(@NotNull String permission);

        @NotNull Builder description(@NotNull String description);

        /**
         * Registers a synchronous handler. Mutually exclusive with
         * {@link #futureHandler}; the last call wins.
         */
        @NotNull Builder handler(@NotNull Consumer<GrimCommandContext> handler);

        /**
         * Registers an async handler returning a future that completes after the
         * command's logic finishes. Cloud awaits the future.
         */
        @NotNull Builder futureHandler(@NotNull Function<GrimCommandContext, CompletableFuture<Void>> handler);

        @NotNull Built build();
    }

    /**
     * Opaque handle for a built command. Pass to
     * {@link ac.grim.grimac.api.command.CommandRegistry#registerBuilt}.
     */
    public sealed interface Built permits BuiltImpl {
    }

    /**
     * Captured spec read by the internal Cloud bridge.
     *
     * <p>This type is part of the public API surface for the bridge to consume,
     * but extensions should treat it as opaque — its shape may evolve.
     */
    @ApiStatus.Internal
    public record Spec(
            @NotNull String rootName,
            @NotNull String[] rootAliases,
            @NotNull List<Step> steps,
            @NotNull Map<String, FlagSpec> flags,
            @Nullable String permission,
            @NotNull String description,
            @Nullable Consumer<GrimCommandContext> handler,
            @Nullable Function<GrimCommandContext, CompletableFuture<Void>> futureHandler
    ) {
    }

    @ApiStatus.Internal
    public sealed interface Step permits LiteralStep, ArgumentStep {
    }

    @ApiStatus.Internal
    public record LiteralStep(@NotNull String name, @NotNull String[] aliases) implements Step {
    }

    @ApiStatus.Internal
    public record ArgumentStep(@NotNull String name, @NotNull ArgumentParser<?> parser, boolean required) implements Step {
    }

    @ApiStatus.Internal
    public record FlagSpec(@NotNull String name, @NotNull String[] aliases, @Nullable ArgumentParser<?> parser) {
    }

    private static final class BuilderImpl implements Builder {
        private final String name;
        private final String[] aliases;
        private final List<Step> steps = new ArrayList<>();
        private final Map<String, FlagSpec> flags = new LinkedHashMap<>();
        private @Nullable String permission;
        private String description = "";
        private @Nullable Consumer<GrimCommandContext> handler;
        private @Nullable Function<GrimCommandContext, CompletableFuture<Void>> futureHandler;

        BuilderImpl(String name, String[] aliases) {
            this.name = name;
            this.aliases = aliases.clone();
        }

        @Override
        public @NotNull Builder literal(@NotNull String name, @NotNull String... aliases) {
            steps.add(new LiteralStep(name, aliases.clone()));
            return this;
        }

        @Override
        public <T> @NotNull Builder required(@NotNull String name, @NotNull ArgumentParser<T> parser) {
            steps.add(new ArgumentStep(name, parser, true));
            return this;
        }

        @Override
        public <T> @NotNull Builder optional(@NotNull String name, @NotNull ArgumentParser<T> parser) {
            steps.add(new ArgumentStep(name, parser, false));
            return this;
        }

        @Override
        public @NotNull Builder flag(@NotNull String name, @NotNull String... aliases) {
            flags.put(name, new FlagSpec(name, aliases.clone(), null));
            return this;
        }

        @Override
        public <T> @NotNull Builder valueFlag(@NotNull String name, @NotNull ArgumentParser<T> parser, @NotNull String... aliases) {
            flags.put(name, new FlagSpec(name, aliases.clone(), parser));
            return this;
        }

        @Override
        public @NotNull Builder permission(@NotNull String permission) {
            this.permission = permission;
            return this;
        }

        @Override
        public @NotNull Builder description(@NotNull String description) {
            this.description = description;
            return this;
        }

        @Override
        public @NotNull Builder handler(@NotNull Consumer<GrimCommandContext> handler) {
            this.handler = handler;
            this.futureHandler = null;
            return this;
        }

        @Override
        public @NotNull Builder futureHandler(@NotNull Function<GrimCommandContext, CompletableFuture<Void>> handler) {
            this.futureHandler = handler;
            this.handler = null;
            return this;
        }

        @Override
        public @NotNull Built build() {
            if (handler == null && futureHandler == null) {
                throw new IllegalStateException("Command " + name + " has no handler");
            }
            Spec spec = new Spec(
                    name,
                    aliases.clone(),
                    Collections.unmodifiableList(new ArrayList<>(steps)),
                    Collections.unmodifiableMap(new LinkedHashMap<>(flags)),
                    permission,
                    description,
                    handler,
                    futureHandler
            );
            return new BuiltImpl(spec);
        }
    }
}
