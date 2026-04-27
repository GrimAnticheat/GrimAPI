package ac.grim.grimac.api.command;

import ac.grim.grimac.api.command.builder.GrimCommand;
import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Entry point for extensions to register commands. Registrations are bound to
 * the supplied {@link GrimPlugin} owner — when that plugin disables, every
 * command it owns is swept automatically. Mirrors the {@code EventBus}
 * lifecycle contract.
 *
 * <p>Two mount strategies:
 * <ul>
 *   <li>{@link #register(GrimPlugin, AbstractCommand)} — top-level. Mounts as
 *       {@code /<command.getName()>}.</li>
 *   <li>{@link #registerUnderGrim(GrimPlugin, AbstractCommand)} — splices into
 *       the {@code /grim} root. Mounts as {@code /grim <command.getName()>}.
 *       Rejects names that collide with built-in subcommands.</li>
 * </ul>
 *
 * <p>Permission strings of the form {@code "?"} (see
 * {@link AbstractCommand#suggestPerm()}) are auto-derived from the literal
 * path. Top-level commands derive into {@code <root>.command.<dotted.path>};
 * {@code registerUnderGrim} derives into {@code grim.command.<dotted.path>}
 * (matching grim-internal commands).
 */
public interface CommandRegistry {

    /**
     * Registers a command tree at the top level. The root command's
     * {@link AbstractCommand#getName()} (and its aliases) become root-level
     * Minecraft commands.
     *
     * @throws IllegalStateException if a top-level command with the same name
     *                               already exists
     */
    void register(@NotNull GrimPlugin owner, @NotNull AbstractCommand command);

    /**
     * Registers a command tree as a child of the built-in {@code /grim} root.
     * The command's {@link AbstractCommand#getName()} appears as a subcommand
     * of {@code /grim}.
     *
     * @throws IllegalStateException if the name collides with a built-in
     *                               {@code /grim} subcommand
     */
    void registerUnderGrim(@NotNull GrimPlugin owner, @NotNull AbstractCommand command);

    /**
     * Registers a command built via the {@link GrimCommand#builder} facade.
     * Use this for commands that need typed argument parsers, flags, or async
     * handlers — the Cloud-shaped path. For hierarchy-style class-per-command
     * authoring, use {@link #register(GrimPlugin, AbstractCommand)} instead.
     */
    void registerBuilt(@NotNull GrimPlugin owner, @NotNull GrimCommand.Built command);

    /**
     * Removes a single previously registered command. Normally not needed —
     * disable cleanup ({@link #unregisterAll(GrimPlugin)}) handles this.
     */
    void unregister(@NotNull AbstractCommand command);

    /**
     * Removes every command owned by {@code owner}. Called automatically when
     * the owning plugin disables.
     */
    void unregisterAll(@NotNull GrimPlugin owner);
}
