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
