package ac.grim.grimac.internal.storage.verbose;

import ac.grim.grimac.api.storage.verbose.Verbose;
import ac.grim.grimac.api.storage.verbose.VerboseFormatter;
import ac.grim.grimac.api.storage.verbose.VerboseRenderContext;
import ac.grim.grimac.api.storage.verbose.VerboseSchema;
import ac.grim.grimac.internal.storage.checks.CheckRegistry;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

@ApiStatus.Internal
public interface VerboseRegistry {

    void register(@NotNull String stableKey, @NotNull VerboseSchema schema);

    void registerFormatter(@NotNull String stableKey, @NotNull VerboseFormatter formatter);

    /**
     * Intern a template-described verbose format on first use. Idempotent and
     * cheap when already registered; on a new registration the check is
     * interned, the schema record (layout + template text) is persisted, and
     * the change listener fires so the startup manifest can be republished.
     */
    void registerTemplate(
            @NotNull String stableKey,
            @NotNull String checkName,
            @Nullable String description,
            @Nullable String pluginVersion,
            @NotNull Verbose verbose);

    /** Invoked after a new template registration lands; replaces any previous listener. */
    void onChange(@Nullable Runnable listener);

    @NotNull String render(@NotNull String stableKey, byte @NotNull [] data, @NotNull VerboseRenderContext ctx);

    @NotNull Map<Integer, Integer> checkIdVersions(@NotNull CheckRegistry checks);

    @Nullable VerboseFormatter codeFormatter(int flavor, int checkId, int version);

    @Nullable VerboseSchema.Layout layout(int flavor, int checkId, int version);
}
