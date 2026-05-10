package ac.grim.grimac.api.storage.extension;

import ac.grim.grimac.api.storage.category.AccessPattern;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.model.ExtensionStorageRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Factory and marker for extension-scoped binary categories.
 */
@ApiStatus.Experimental
public final class ExtensionStorageCategories {
    private ExtensionStorageCategories() {}

    public static @NotNull ExtensionCategory binary(
            @NotNull String extensionId,
            @NotNull String localId,
            @NotNull AccessPattern accessPattern,
            @NotNull EnumSet<Capability> required) {
        return new ExtensionCategory(extensionId, localId, accessPattern, required);
    }

    public static boolean isExtensionCategory(Category<?> category) {
        return category instanceof ExtensionCategory;
    }

    public static @NotNull String normalizeId(@NotNull String field, @NotNull String input) {
        Objects.requireNonNull(input, field);
        String trimmed = input.trim().toLowerCase(Locale.ROOT);
        if (trimmed.isEmpty()) throw new IllegalArgumentException(field + " cannot be blank");
        StringBuilder out = new StringBuilder(trimmed.length());
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') out.append(c);
            else out.append('_');
        }
        return out.toString();
    }

    public static final class ExtensionCategory implements Category<ExtensionStorageEvent> {
        private final String extensionId;
        private final String localId;
        private final String id;
        private final AccessPattern accessPattern;
        private final EnumSet<Capability> required;

        private ExtensionCategory(String extensionId, String localId,
                                  AccessPattern accessPattern, EnumSet<Capability> required) {
            this.extensionId = normalizeId("extensionId", extensionId);
            this.localId = normalizeId("localId", localId);
            this.id = "ext/" + this.extensionId + "/" + this.localId;
            this.accessPattern = Objects.requireNonNull(accessPattern, "accessPattern");
            this.required = required == null || required.isEmpty()
                    ? EnumSet.of(Capability.EXTENSION_STORAGE)
                    : EnumSet.copyOf(required);
            this.required.add(Capability.EXTENSION_STORAGE);
        }

        public @NotNull String extensionId() { return extensionId; }

        public @NotNull String localId() { return localId; }

        @Override public @NotNull String id() { return id; }

        @Override public @NotNull Class<ExtensionStorageEvent> eventType() { return ExtensionStorageEvent.class; }

        @Override public @NotNull Supplier<ExtensionStorageEvent> newEvent() { return ExtensionStorageEvent::new; }

        @Override public @NotNull Class<?> queryResultType() { return ExtensionStorageRecord.class; }

        @Override public @NotNull EnumSet<Capability> requiredCapabilities() { return EnumSet.copyOf(required); }

        @Override public @NotNull AccessPattern accessPattern() { return accessPattern; }
    }
}
