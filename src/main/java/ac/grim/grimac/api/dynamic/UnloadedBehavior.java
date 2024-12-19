package ac.grim.grimac.api.dynamic;

import java.lang.reflect.Method;

public interface UnloadedBehavior {
    Object handleUnloadedCall(Method method, Object[] args);
}