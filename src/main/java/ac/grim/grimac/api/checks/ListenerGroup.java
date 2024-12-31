package ac.grim.grimac.api.checks;

import ac.grim.grimac.api.AbstractProcessor;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;

import java.util.Map;
@Getter
public enum ListenerGroup {

    UNSET("unset", false), // Unknown listener group
    NONE("none", false), // Supposed to not have any listeners
    PRE_PREDICTION("pre_prediction"), // Handle before predictions
    PACKET("packet"); // Default packet listener

    private final static Map<String, ListenerGroup> MAP;

    private final String name;
    private final boolean listening;

    static {
        var builder = ImmutableMap.<String, ListenerGroup>builder();
        for (ListenerGroup type : values()) builder.put(type.getName(), type);
        MAP = builder.build();
    }

    public boolean isEqual(AbstractProcessor processor) {
        return processor != null && name.equals(processor.getListenerGroup());
    }

    public static ListenerGroup fromName(String name) {
        return MAP.getOrDefault(name, UNSET);
    }

    ListenerGroup(String name) {
        this.name = name;
        this.listening = true;
    }

    ListenerGroup(String name, boolean listening) {
        this.name = name;
        this.listening = listening;
    }

}
