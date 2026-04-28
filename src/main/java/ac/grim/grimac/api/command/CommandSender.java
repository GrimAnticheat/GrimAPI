package ac.grim.grimac.api.command;

import ac.grim.grimac.api.GrimUser;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * Platform-neutral command sender surface exposed to API consumers.
 *
 * <p>The internal {@code Sender} type used by Grim's command implementation extends
 * this interface, so any internal sender can be passed straight to extension
 * handlers. Extensions writing commands against the public API see only this type.
 *
 * <p>The console UUID is fixed to {@link #CONSOLE_UUID} across every platform.
 */
public interface CommandSender {

    /** UUID returned by {@link #getUniqueId()} for the console. */
    UUID CONSOLE_UUID = new UUID(0, 0);

    /** Name returned by {@link #getName()} for the console. */
    String CONSOLE_NAME = "Console";

    String getName();

    UUID getUniqueId();

    void sendMessage(String message);

    boolean hasPermission(String permission);

    void performCommand(String commandLine);

    boolean isConsole();

    boolean isPlayer();

    /**
     * Categorical view of this sender, cross-platform. Default implementation
     * derives from the existing {@link #isPlayer()} / {@link #isConsole()}
     * checks; platform-specific senders should override to disambiguate
     * {@link SenderKind#REMOTE_CONSOLE}, {@link SenderKind#COMMAND_BLOCK},
     * {@link SenderKind#FUNCTION}, etc.
     */
    default @org.jetbrains.annotations.NotNull SenderKind getKind() {
        if (isPlayer()) return SenderKind.PLAYER;
        if (isConsole()) return SenderKind.CONSOLE;
        return SenderKind.OTHER;
    }

    /** True iff this sender is RCON (a network-attached console). Default: derived from {@link #getKind()}. */
    default boolean isRemoteConsole() {
        return getKind() == SenderKind.REMOTE_CONSOLE;
    }

    /** True iff this sender is a command block in the world. Default: derived from {@link #getKind()}. */
    default boolean isCommandBlock() {
        return getKind() == SenderKind.COMMAND_BLOCK;
    }

    default boolean isValid() {
        return true;
    }

    /**
     * Resolves the {@link GrimUser} associated with this sender, if any.
     *
     * @return the GrimUser if this sender is a tracked player, null otherwise
     *         (console, or a player that has not been registered yet)
     */
    @Nullable GrimUser asGrimUser();
}
