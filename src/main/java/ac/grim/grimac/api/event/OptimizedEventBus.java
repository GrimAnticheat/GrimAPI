package ac.grim.grimac.api.event;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class OptimizedEventBus implements EventBus {
    // ordinarily this would just be a hashmap since we don't modify it on different threads, but Folia forces us to make it concurrent
    private final Map<Class<? extends GrimEvent>, List<OptimizedListener>> listenerMap = new ConcurrentHashMap<>();
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();

    @Override
    public void registerListeners(Object listener) {
        registerMethods(listener, listener.getClass());
    }

    @Override
    public void registerStaticListeners(Class<?> clazz) {
        registerMethods(null, clazz);
    }

    private void registerMethods(Object instance, Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            GrimEventHandler annotation = method.getAnnotation(GrimEventHandler.class);
            if (annotation != null && method.getParameterCount() == 1) {
                Class<?> eventType = method.getParameterTypes()[0];
                if (GrimEvent.class.isAssignableFrom(eventType)) {
                    try {
                        // Skip instance methods when registering static listeners
                        if (instance == null && !Modifier.isStatic(method.getModifiers())) {
                            continue;
                        }

                        MethodHandle handle = lookup.unreflect(method);
                        OptimizedListener optimizedListener = new OptimizedListener(
                                instance, handle, annotation.priority(), annotation.ignoreCancelled(), method.getDeclaringClass()
                        );

                        // Register for the event and its parent classes
                        Class<?> currentEventType = eventType;
                        while (GrimEvent.class.isAssignableFrom(currentEventType)) {
                            listenerMap.computeIfAbsent((Class<? extends GrimEvent>) currentEventType, k -> new CopyOnWriteArrayList<>())
                                    .add(optimizedListener);
                            currentEventType = currentEventType.getSuperclass();
                        }
                    } catch (IllegalAccessException e) {
                        System.out.println("Failed to register listener for " + eventType.getName());
                        e.printStackTrace();
                    }
                }
            }
        }
        // Sort listeners by priority (descending)
        listenerMap.values().forEach(list -> list.sort((a, b) -> Integer.compare(b.priority, a.priority)));
    }

    @Override
    public void unregisterListeners(Object listener) {
        listenerMap.values().forEach(list -> list.removeIf(l -> l.instance == listener));
    }

    @Override
    public void unregisterStaticListeners(Class<?> clazz) {
        listenerMap.values().forEach(list -> list.removeIf(l -> l.instance == null && l.declaringClass == clazz));
    }

    @Override
    public void post(GrimEvent event) {
        // Validate async/sync context
//        if (event.isAsync() && isMainThread()) {
//            throw new IllegalStateException(event.getEventName() + " is async but called on main thread");
//        } else if (!event.isAsync() && !isMainThread()) {
//            throw new IllegalStateException(event.getEventName() + " is sync but called on async thread");
//        }

        // Post to the event and its parent classes
        Class<?> currentEventType = event.getClass();
        while (GrimEvent.class.isAssignableFrom(currentEventType)) {
            List<OptimizedListener> listeners = listenerMap.get(currentEventType);
            if (listeners != null) {
                for (OptimizedListener listener : listeners) {
                    try {
                        // Skip if event is cancelled and listener doesn't ignore cancelled
                        if (event.isCancelled() && !listener.ignoreCancelled) {
                            continue;
                        }

                        if (listener.instance != null) {
                            listener.handle.invoke(listener.instance, event);
                        } else {
                            listener.handle.invoke(event);
                        }
                    } catch (Throwable throwable) {
                        System.out.println("Error handling event " + event.getEventName());
                        throwable.printStackTrace();
                    }
                }
            }
            currentEventType = currentEventType.getSuperclass();
        }
    }


    private static class OptimizedListener {
        final Object instance; // null for static methods
        final MethodHandle handle;
        final int priority;
        final boolean ignoreCancelled;
        final Class<?> declaringClass;

        OptimizedListener(Object instance, MethodHandle handle, int priority, boolean ignoreCancelled, Class<?> declaringClass) {
            this.instance = instance;
            this.handle = handle;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.declaringClass = declaringClass;
        }
    }
}