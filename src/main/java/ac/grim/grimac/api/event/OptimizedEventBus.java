package ac.grim.grimac.api.event;

import ac.grim.grimac.api.GrimPlugin;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class OptimizedEventBus implements EventBus {
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    // ordinarily this would just be a hashmap since we don't modify it on different threads, but Folia forces us to make it concurrent
    private final Map<GrimPlugin, Map<Class<? extends GrimEvent>, List<OptimizedListener>>> listenerMap = new ConcurrentHashMap<>();

    @Override
    public void registerAnnotatedListeners(GrimPlugin plugin, Object listener) {
        registerMethods(plugin, listener, listener.getClass());
    }

    @Override
    public void registerStaticAnnotatedListeners(GrimPlugin plugin, Class<?> clazz) {
        registerMethods(plugin, null, clazz);
    }

    private void registerMethods(GrimPlugin plugin, Object instance, Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            GrimEventHandler annotation = method.getAnnotation(GrimEventHandler.class);
            if (annotation != null && method.getParameterCount() == 1) {
                Class<?> eventType = method.getParameterTypes()[0];
                if (GrimEvent.class.isAssignableFrom(eventType)) {
                    try {
                        if (instance == null && !Modifier.isStatic(method.getModifiers())) {
                            continue;
                        }

                        // Ensure method is accessible
                        method.setAccessible(true);
                        MethodHandle handle = lookup.unreflect(method);

                        // Wrap MethodHandle in a lambda with proper type safety
                        GrimEventListener<GrimEvent> listener = event -> {
                            try {
                                if (instance != null) {
                                    handle.invoke(instance, event);
                                } else {
                                    handle.invoke(event);
                                }
                            } catch (Throwable throwable) {
                                throw new RuntimeException("Failed to invoke listener for " + eventType.getName(), throwable);
                            }
                        };

                        // Register for the event and its parent classes
                        Class<?> currentEventType = eventType;
                        while (GrimEvent.class.isAssignableFrom(currentEventType)) {
                            OptimizedListener optimizedListener = new OptimizedListener(listener, annotation.priority(), annotation.ignoreCancelled(), method.getDeclaringClass(), instance);
                            listenerMap.computeIfAbsent(plugin, k -> new ConcurrentHashMap<>())
                                    .computeIfAbsent((Class<? extends GrimEvent>) currentEventType, k -> new CopyOnWriteArrayList<>())
                                    .add(optimizedListener);
                            currentEventType = currentEventType.getSuperclass();
                        }
                    } catch (IllegalAccessException e) {
                        System.err.println("Failed to register listener for " + eventType.getName() + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
        // Sort listeners by priority (descending)
        listenerMap.getOrDefault(plugin, new ConcurrentHashMap<>())
                .values().forEach(list -> list.sort((a, b) -> Integer.compare(b.priority, a.priority)));
    }

    @Override
    public void unregisterListeners(GrimPlugin plugin, Object listener) {
        Map<Class<? extends GrimEvent>, List<OptimizedListener>> pluginListeners = listenerMap.get(plugin);
        if (pluginListeners != null) {
            pluginListeners.values().forEach(list -> list.removeIf(l -> l.instance != null && l.instance.equals(listener)));
            cleanupEmptyEntries(plugin);
        }
    }

    @Override
    public void unregisterStaticListeners(GrimPlugin plugin, Class<?> clazz) {
        Map<Class<? extends GrimEvent>, List<OptimizedListener>> pluginListeners = listenerMap.get(plugin);
        if (pluginListeners != null) {
            pluginListeners.values().forEach(list -> list.removeIf(l -> l.instance == null && l.declaringClass.equals(clazz)));
            cleanupEmptyEntries(plugin);
        }
    }

    @Override
    public void unregisterAllListeners(GrimPlugin plugin) {
        listenerMap.remove(plugin);
    }

    @Override
    public void unregisterListener(GrimPlugin plugin, GrimEventListener<?> listener) {
        Map<Class<? extends GrimEvent>, List<OptimizedListener>> pluginListeners = listenerMap.get(plugin);
        if (pluginListeners != null) {
            pluginListeners.values().forEach(list -> list.removeIf(l -> l.listener.equals(listener)));
            cleanupEmptyEntries(plugin);
        }
    }

    @Override
    public void post(GrimEvent event) {
//        if (event.isAsync() && isMainThread()) {
//            throw new IllegalStateException(event.getEventName() + " is async but called on main thread");
//        } else if (!event.isAsync() && !isMainThread()) {
//            throw new IllegalStateException(event.getEventName() + " is sync but called on async thread");
//        }

        // Post to the event and its parent classes
        Class<?> currentEventType = event.getClass();
        while (GrimEvent.class.isAssignableFrom(currentEventType)) {
            for (Map<Class<? extends GrimEvent>, List<OptimizedListener>> pluginListeners : listenerMap.values()) {
                List<OptimizedListener> listeners = pluginListeners.get(currentEventType);
                if (listeners != null) {
                    for (OptimizedListener listener : listeners) {
                        try {
                            if (event.isCancelled() && !listener.ignoreCancelled) {
                                continue;
                            }

                            GrimEventListener<GrimEvent> eventListener = listener.listener;
                            eventListener.handle(event);
                        } catch (Throwable throwable) {
                            System.err.println("Error handling event " + event.getEventName() + ": " + throwable.getMessage());
                            throwable.printStackTrace();
                        }
                    }
                }
            }
            currentEventType = currentEventType.getSuperclass();
        }
    }

    @Override
    public <T extends GrimEvent> void subscribe(GrimPlugin plugin, Class<T> eventType, GrimEventListener<T> listener, int priority, boolean ignoreCancelled, Class<?> declaringClass) {
        OptimizedListener optimizedListener = new OptimizedListener(listener, priority, ignoreCancelled, declaringClass, null);
        listenerMap.computeIfAbsent(plugin, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>())
                .add(optimizedListener);
        // Sort listeners by priority (descending)
        listenerMap.get(plugin).get(eventType).sort((a, b) -> Integer.compare(b.priority, a.priority));
    }

    private void cleanupEmptyEntries(GrimPlugin plugin) {
        Map<Class<? extends GrimEvent>, List<OptimizedListener>> pluginListeners = listenerMap.get(plugin);
        if (pluginListeners != null) {
            pluginListeners.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            if (pluginListeners.isEmpty()) {
                listenerMap.remove(plugin);
            }
        }
    }

    private static class OptimizedListener {
        final GrimEventListener<GrimEvent> listener;
        final int priority;
        final boolean ignoreCancelled;
        final Class<?> declaringClass;
        final Object instance;

        @SuppressWarnings("unchecked")
        OptimizedListener(GrimEventListener<?> listener, int priority, boolean ignoreCancelled, Class<?> declaringClass, Object instance) {
            this.listener = (GrimEventListener<GrimEvent>) listener;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.declaringClass = declaringClass;
            this.instance = instance;
        }
    }
}