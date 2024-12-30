package ac.grim.grimac.api.debug;

import java.util.function.Supplier;

public interface Debuggable {

    String identifier();

    void debug(Supplier<String> details);

}
