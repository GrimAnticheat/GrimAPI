package ac.grim.grimac.api.storage.query;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record Cursor(String token) {

    public Cursor {
        if (token == null) throw new IllegalArgumentException("token");
    }
}
