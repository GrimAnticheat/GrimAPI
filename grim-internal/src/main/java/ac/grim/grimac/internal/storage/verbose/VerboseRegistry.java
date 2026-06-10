package ac.grim.grimac.internal.storage.verbose;

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

    @NotNull String render(@NotNull String stableKey, byte @NotNull [] data, @NotNull VerboseRenderContext ctx);

    @NotNull Map<Integer, Integer> checkIdVersions(@NotNull CheckRegistry checks);

    @Nullable VerboseFormatter codeFormatter(int flavor, int checkId, int version);

    @Nullable VerboseSchema.Layout layout(int flavor, int checkId, int version);
}
