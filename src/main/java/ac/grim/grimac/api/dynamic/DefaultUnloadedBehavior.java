package ac.grim.grimac.api.dynamic;

import java.lang.reflect.Method;

// Default implementations
public class DefaultUnloadedBehavior implements UnloadedBehavior {
    public static final UnloadedBehavior INSTANCE = new DefaultUnloadedBehavior();
    
    @Override
    public Object handleUnloadedCall(Method method, Object[] args) {
      Class<?> returnType = method.getReturnType();
      if (returnType == boolean.class) {
        return false;
      }
      if (returnType == int.class) {
        return 0;
      }
      if (returnType == void.class) {
        return null;
      }
        return null;
    }
}