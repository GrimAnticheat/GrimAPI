package ac.grim.grimac.api.dynamic;

import java.lang.reflect.Method;

/**
 * Default implementation that returns safe "no-op" values for unloaded checks.
 * - boolean methods return false
 * - numeric methods return 0
 * - void methods do nothing
 * - object methods return null
 */
public class DefaultUnloadedBehavior implements UnloadedBehavior {
  public static final UnloadedBehavior INSTANCE = new DefaultUnloadedBehavior();

  @Override
  public Object handleUnloadedCall(Method method, Object[] args) {
    Class<?> returnType = method.getReturnType();
    if (returnType == boolean.class) return false;
    if (returnType == int.class) return 0;
    if (returnType == void.class) return null;
    return null;
  }
}