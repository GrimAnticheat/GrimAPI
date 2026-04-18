package ac.grim.grimac.api.storage.retention;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public enum RetentionAction {
    DELETE,
    KEEP,
    HANDED_OFF
}
