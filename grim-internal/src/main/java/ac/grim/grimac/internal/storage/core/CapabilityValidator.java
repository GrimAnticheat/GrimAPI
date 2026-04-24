package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Startup check: every routed category is actually supported by its backend and the
 * backend declares every capability the category requires.
 * <p>
 * Failures are collected and thrown as a single {@link ValidationException} so the
 * operator sees all problems at once.
 */
@ApiStatus.Internal
public final class CapabilityValidator {

    private CapabilityValidator() {}

    public static void validate(Map<Category<?>, Backend> routing) {
        List<String> errors = new ArrayList<>();
        for (Map.Entry<Category<?>, Backend> e : routing.entrySet()) {
            Category<?> cat = e.getKey();
            Backend b = e.getValue();
            if (!b.supportedCategories().contains(cat)) {
                errors.add("Category '" + cat.id()
                        + "' routed to backend '" + b.id()
                        + "' which does not declare support for it.");
                continue;
            }
            EnumSet<Capability> missing = EnumSet.copyOf(cat.requiredCapabilities());
            missing.removeAll(b.capabilities());
            if (!missing.isEmpty()) {
                errors.add("Category '" + cat.id()
                        + "' requires capabilities " + missing
                        + " not provided by backend '" + b.id() + "'.");
            }
        }
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder("[grim-datastore] routing validation failed:\n");
            for (String err : errors) sb.append("  - ").append(err).append('\n');
            sb.append("Edit database.yml routing or the backend config and restart.");
            throw new ValidationException(sb.toString());
        }
    }

    public static final class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }
}
