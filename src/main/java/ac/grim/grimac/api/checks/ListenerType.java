package ac.grim.grimac.api.checks;

import lombok.Getter;

@Getter
public enum ListenerType {

    PACKET("packet"),
    SEND("send"),
    RECEIVE("receive"),
    DIG("dig"),
    POST_PREDICTION("post_prediction"),
    ROTATION("rotation"),
    BLOCK_PLACE("block_place"),
    PRE_VIA("pre_via"),
    PRE_PREDICTION("pre_prediction"),
    PSEUDO("pseudo", false),
    POSITION("position"),
    VEHICLE("vehicle"),
    NONE("none", false),
    UNSET("unknown", false),
    ;

    private final String name;
    private final boolean listening;

    ListenerType(String name) {
        this.name = name;
        this.listening = true;
    }

    ListenerType(String name, boolean listening) {
        this.name = name;
        this.listening = listening;
    }

}
