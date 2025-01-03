package ac.grim.grimac.api.instance;

import ac.grim.grimac.api.GrimAbstractAPI;
import org.jetbrains.annotations.NotNull;

import static org.jetbrains.annotations.ApiStatus.Internal;

public final class GrimProvider {

    private static GrimAbstractAPI instance;

    public static @NotNull GrimAbstractAPI getInstance() {
        if (instance == null) throw new IllegalStateException("GrimAC API has not been initialized yet!");
        return instance;
    }

    @Internal
    static void register(GrimAbstractAPI instance) {
        GrimProvider.instance = instance;
    }

    @Internal
    private GrimProvider() {
        throw new UnsupportedOperationException("This class cannot be instantiated.");
    }

}
