package ac.grim.grimac.api.debug;

import java.util.function.Supplier;

public interface AbstractDebugManager {

    void handleDebug(Debuggable debuggable, Supplier<String> returner);

}
