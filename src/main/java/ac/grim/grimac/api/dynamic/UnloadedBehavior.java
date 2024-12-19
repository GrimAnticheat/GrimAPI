package ac.grim.grimac.api.dynamic;

import java.lang.reflect.Method;

/**
 * Defines how an unloaded check should behave when its methods are called.
 * This allows for graceful degradation when checks are disabled/unloaded.
 */
public interface UnloadedBehavior {
    /**
     * Handle a method call on an unloaded check
     *
     * @param method The method being called
     * @param args The arguments passed to the method
     * @return The value to return from the method call
     */
    Object handleUnloadedCall(Method method, Object[] args);
}