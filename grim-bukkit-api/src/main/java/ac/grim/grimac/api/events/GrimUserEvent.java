package ac.grim.grimac.api.events;

import ac.grim.grimac.api.GrimUser;

@Deprecated(since = "1.2.1.0", forRemoval = true)
public interface GrimUserEvent {

    GrimUser getUser();

    default GrimUser getPlayer() {
        return getUser();
    }

}
