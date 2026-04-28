package ac.grim.grimac.api.command;

/**
 * Categorical view of a {@link CommandSender}, normalized across platforms.
 *
 * <p>Bukkit hangs sender variants off a class hierarchy
 * ({@code Player}, {@code ConsoleCommandSender}, {@code RemoteConsoleCommandSender},
 * {@code BlockCommandSender}, …); Fabric uses one {@code CommandSourceStack}
 * with a polymorphic {@code source} field. {@link SenderKind} is the
 * cross-platform projection — extension authors switch on this rather than
 * doing platform-specific {@code instanceof} checks.
 *
 * <p>{@code FUNCTION} is Fabric-specific (function-file execution). On Bukkit
 * the closest analog is {@code ConsoleCommandSender}, so callers that don't
 * care about the distinction can fall through OTHER cases together.
 */
public enum SenderKind {

    /** A real player executing the command. */
    PLAYER,

    /**
     * A non-player entity executing the command (Fabric: any
     * {@code sender.getEntity()} that isn't a {@code ServerPlayer} —
     * armour stand, mob, etc. driven by {@code /execute as}). On Bukkit
     * this is currently treated as {@link #OTHER} since vanilla Bukkit
     * doesn't expose entity command senders directly.
     */
    NON_PLAYER_ENTITY,

    /** The local server console / dedicated server. */
    CONSOLE,

    /** RCON. Distinct from {@link #CONSOLE} so callers can refuse remote ops. */
    REMOTE_CONSOLE,

    /** A command block placed in the world is executing the command. */
    COMMAND_BLOCK,

    /**
     * A {@code .mcfunction} file is executing the command. Fabric-specific —
     * Bukkit reports these as {@link #CONSOLE}.
     */
    FUNCTION,

    /** Anything else — proxy senders, plugin-spawned senders, future variants. */
    OTHER;
}
