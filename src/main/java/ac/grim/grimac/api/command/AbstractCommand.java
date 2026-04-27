package ac.grim.grimac.api.command;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Base class for hierarchical commands. Subclass and configure in the constructor;
 * register via {@link CommandRegistry#register(ac.grim.grimac.api.plugin.GrimPlugin, AbstractCommand)}
 * (top-level) or {@link CommandRegistry#registerUnderGrim(ac.grim.grimac.api.plugin.GrimPlugin, AbstractCommand)}
 * (mounted under {@code /grim}).
 *
 * <p>Each instance represents one node in a command tree. Permissions in the
 * permissions list are evaluated with OR semantics: any matching entry grants
 * access. The literal {@code "?"} (also written {@link #suggestPerm()}) is replaced
 * at registration time with a permission derived from the literal path —
 * {@code grim.command.<dotted.path>} for grim-internal commands, or
 * {@code <root>.command.<dotted.path>} for extension-registered roots. The
 * literal {@code "grim.dev"} is also recognized — it gates the command behind
 * the developer check, and a command with only {@code grim.dev} is hidden from
 * tab-complete for non-developers.
 *
 * <p>Override {@link #onCommand(CommandSender, String, String[])} to handle the
 * command and {@link #onTabComplete(AbstractCommand, CommandSender, String, String[])}
 * for tab completion. Use {@link #registerSubCommand(AbstractCommand)} in the
 * constructor to attach children.
 */
public abstract class AbstractCommand {

    /**
     * Placeholder permission string. At registration time this is replaced with
     * a permission derived from the literal path.
     */
    public static final String SUGGEST = "?";

    /** Returns {@link #SUGGEST}, mirrored from the legacy 3.0 API. */
    public static String suggestPerm() {
        return SUGGEST;
    }

    /**
     * Returns a string of {@code n} {@code ?} characters, mirrored from the
     * legacy 3.0 API. The level is preserved for compatibility but is not
     * meaningful in this implementation — every {@code ?}-only string resolves
     * to the same derived permission.
     */
    public static String suggestPerm(int level) {
        if (level < 1) throw new IllegalArgumentException("Suggest perm level must be at least 1");
        StringBuilder sb = new StringBuilder(level);
        for (int i = 0; i < level; i++) sb.append('?');
        return sb.toString();
    }

    private static final Pattern NAME_FILTER = Pattern.compile("[ @.]");

    private final String name;
    private final List<String> aliases;
    private final List<String> permissions;
    private final boolean requiresPlayer;

    private final Map<String, AbstractCommand> children = new LinkedHashMap<>();
    private AbstractCommand parent;
    private String description = "";
    private String tip = "";
    private boolean hidden = false;

    protected AbstractCommand(@NotNull String name, @NotNull String permission) {
        this(name, Collections.emptyList(), Collections.singletonList(permission), false);
    }

    protected AbstractCommand(@NotNull String name, @NotNull String permission, boolean requiresPlayer) {
        this(name, Collections.emptyList(), Collections.singletonList(permission), requiresPlayer);
    }

    protected AbstractCommand(@NotNull String name, @NotNull List<String> permissions) {
        this(name, Collections.emptyList(), permissions, false);
    }

    protected AbstractCommand(@NotNull String name, @NotNull List<String> permissions, boolean requiresPlayer) {
        this(name, Collections.emptyList(), permissions, requiresPlayer);
    }

    protected AbstractCommand(@NotNull String name, @NotNull List<String> aliases,
                              @NotNull List<String> permissions, boolean requiresPlayer) {
        this.name = name;
        this.aliases = List.copyOf(aliases);
        this.permissions = List.copyOf(permissions);
        this.requiresPlayer = requiresPlayer;
    }

    /**
     * Attaches a child command. Call from the constructor of group commands.
     * Children are dispatched as literals at the next path position.
     */
    protected final void registerSubCommand(@NotNull AbstractCommand child) {
        if (child.parent != null) {
            throw new IllegalStateException("Sub-command " + child.name + " already attached to a parent");
        }
        child.parent = this;
        children.put(child.name, child);
        for (String a : child.aliases) {
            children.putIfAbsent(a, child);
        }
    }

    /**
     * Default handler — sends usage information. Override for executable nodes.
     */
    public void onCommand(@NotNull CommandSender sender, @NotNull String label, @NotNull String[] args) {
        sender.sendMessage(usage());
    }

    /**
     * Default tab completer — returns an empty list. Override to provide suggestions.
     *
     * @param command the command being completed (always {@code this}; mirrors
     *                the legacy 3.0 signature for compatibility)
     */
    public List<String> onTabComplete(@NotNull AbstractCommand command, @NotNull CommandSender sender,
                                      @NotNull String label, @NotNull String[] args) {
        return Collections.emptyList();
    }

    /**
     * Allows a sender to bypass session restrictions (e.g., the API-key gate).
     * Override to expand bypass beyond the default of "no bypass."
     */
    public boolean hasSessionBypass(@NotNull CommandSender sender) {
        return false;
    }

    protected final void setDescription(@NotNull String description) {
        this.description = description;
    }

    protected final void setTip(@NotNull String tip) {
        this.tip = tip;
    }

    protected final void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    public final @NotNull String getName() {
        return name;
    }

    public final @NotNull List<String> getAliases() {
        return aliases;
    }

    public final @NotNull List<String> getPermissions() {
        return permissions;
    }

    public final boolean isRequiresPlayer() {
        return requiresPlayer;
    }

    public final @NotNull Map<String, AbstractCommand> getChildren() {
        return Collections.unmodifiableMap(children);
    }

    public final @Nullable AbstractCommand getParent() {
        return parent;
    }

    public final @NotNull String getDescription() {
        return description;
    }

    public final @NotNull String getTip() {
        return tip;
    }

    public final boolean isHidden() {
        return hidden;
    }

    /**
     * Walks the parent chain, returning the dotted path from the root's first
     * child to this node. The root itself contributes no path segment.
     */
    public final @NotNull String derivedPath() {
        Deque<String> stack = new ArrayDeque<>();
        for (AbstractCommand c = this; c != null && c.parent != null; c = c.parent) {
            stack.push(filter(c.name));
        }
        return String.join(".", stack);
    }

    /**
     * Walks the parent chain, returning the space-separated literal path from
     * the root through this node.
     */
    public final @NotNull String fullPath() {
        Deque<String> stack = new ArrayDeque<>();
        for (AbstractCommand c = this; c != null; c = c.parent) {
            stack.push(c.name);
        }
        return String.join(" ", stack);
    }

    /**
     * Returns a usage hint. Override for richer help — by default emits
     * {@code /<full path> <tip>}.
     */
    public @NotNull String usage() {
        return tip.isEmpty() ? "/" + fullPath() : "/" + fullPath() + " " + tip;
    }

    /**
     * @return the argument at {@code idx} (1-based to mirror the legacy 3.0
     * helper that treated {@code args[0]} as the subcommand label), or null if
     * out of range.
     */
    protected final @Nullable String getArgument(@NotNull String[] args, int idx) {
        return idx >= 0 && idx < args.length ? args[idx] : null;
    }

    static String filter(@NotNull String name) {
        return NAME_FILTER.matcher(name).replaceAll("");
    }
}
