package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record RenderOptions(boolean detailed, long groupIntervalMs) {

    public static final RenderOptions DEFAULT = new RenderOptions(false, 30_000L);

    public RenderOptions withDetailed(boolean detailed) {
        return new RenderOptions(detailed, groupIntervalMs);
    }

    public RenderOptions withGroupIntervalMs(long ms) {
        return new RenderOptions(detailed, ms);
    }
}
