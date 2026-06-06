package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Experimental
public interface VerboseFormatter {

    int version();

    void render(@NotNull VerboseBuf in, @NotNull VerboseSink out);
}
