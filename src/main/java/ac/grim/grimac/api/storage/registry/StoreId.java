package ac.grim.grimac.api.storage.registry;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Namespaced identifier for a registered store. Combines an extension/owner
 * namespace with a local name; rendered as {@code "<namespace>:<name>"}.
 * <p>
 * Builtin Grim stores use the {@code "grim"} namespace
 * (e.g. {@code grim:violations}); extensions use their declared id.
 * <p>
 * See {@code .docs/storage-redesign/03-backends-and-adapters.md} for how this
 * replaces the old {@code TableNames} record.
 */
@ApiStatus.Experimental
public record StoreId(@NotNull String namespace, @NotNull String name) {

    public StoreId {
        if (namespace == null || namespace.isEmpty()) throw new IllegalArgumentException("namespace");
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("name");
        if (!isLegal(namespace)) throw new IllegalArgumentException("namespace not [a-z0-9_-]: " + namespace);
        if (!isLegal(name)) throw new IllegalArgumentException("name not [a-z0-9_-]: " + name);
    }

    public static @NotNull StoreId grim(@NotNull String name) {
        return new StoreId("grim", name);
    }

    public @NotNull String qualified() {
        return namespace + ":" + name;
    }

    @Override
    public String toString() {
        return qualified();
    }

    private static boolean isLegal(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_' || c == '-')) return false;
        }
        return true;
    }
}
