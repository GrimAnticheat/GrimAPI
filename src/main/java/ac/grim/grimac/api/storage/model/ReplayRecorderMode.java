package ac.grim.grimac.api.storage.model;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum ReplayRecorderMode {
    VIEWABLE,
    ANALYZABLE,
    VIEWABLE_AND_ANALYZABLE,
    EVERYTHING
}
