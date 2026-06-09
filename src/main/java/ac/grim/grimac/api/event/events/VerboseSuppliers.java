package ac.grim.grimac.api.event.events;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

@ApiStatus.Internal
public final class VerboseSuppliers {
    private VerboseSuppliers() {
    }

    public static @NotNull Supplier<String> constant(String verbose) {
        String value = verbose == null ? "" : verbose;
        return () -> value;
    }

    public static @NotNull Supplier<String> memoize(Supplier<String> supplier) {
        Supplier<String> source = supplier == null ? () -> "" : supplier;
        return new Supplier<>() {
            private String value;
            private boolean computed;

            @Override
            public String get() {
                if (!computed) {
                    value = safeGet(source);
                    computed = true;
                }
                return value;
            }
        };
    }

    private static @NotNull String safeGet(@NotNull Supplier<String> supplier) {
        try {
            String value = supplier.get();
            return value == null ? "" : value;
        } catch (Throwable ignored) {
            return "";
        }
    }
}
