package ac.grim.grimac.api.event;

import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Event bus for registering and dispatching Grim events.
 * Supports both reflective (annotation-based) and explicit (manual) registration.
 * <p>
 * Reflective registration uses {@link ac.grim.grimac.api.event.GrimEventHandler} annotations to automatically
 * register event listeners. Explicit registration uses the {@link #subscribe} methods
 * to manually register listeners with lambdas or method references.
 * <p>
 * All registration methods require a {@link GrimPlugin} instance for plugin lifecycle
 * management. Listeners are associated with plugins and can be unregistered when the
 * plugin is disabled.
 */
public interface EventBus {
    /**
     * Registers instance methods annotated with {@link GrimEventHandler} as event listeners.
     * <p>
     * Example:
     * <pre>{@code
     * public class MyListener {
     *     @GrimEventHandler(priority = 1, ignoreCancelled = true)
     *     public void onFlag(FlagEvent event) {
     *         // Handle flag
     *     }
     * }
     * eventBus.registerAnnotatedListeners(plugin, new MyListener());
     * }</pre>
     *
     * @param plugin   The plugin registering the listener
     * @param listener The listener instance containing annotated methods
     */
    void registerAnnotatedListeners(GrimPlugin plugin, @NotNull Object listener);

    /**
     * Registers static methods annotated with {@link GrimEventHandler} as event listeners.
     * <p>
     * Example:
     * <pre>{@code
     * public class MyStaticListener {
     *     @GrimEventHandler(priority = 1, ignoreCancelled = true)
     *     public static void onFlag(FlagEvent event) {
     *         // Handle flag
     *     }
     * }
     * eventBus.registerStaticAnnotatedListeners(plugin, MyStaticListener.class);
     * }</pre>
     *
     * @param plugin The plugin registering the listener
     * @param clazz  The class containing static annotated methods
     */
    void registerStaticAnnotatedListeners(GrimPlugin plugin, @NotNull Class<?> clazz);

    /**
     * Unregisters all instance listeners associated with the plugin and listener instance.
     *
     * @param plugin   The plugin unregistering the listener
     * @param listener The listener instance to unregister
     */
    void unregisterListeners(GrimPlugin plugin, Object listener);

    /**
     * Unregisters all static listeners associated with the plugin and class.
     *
     * @param plugin The plugin unregistering the listener
     * @param clazz  The class containing static listeners to unregister
     */
    void unregisterStaticListeners(GrimPlugin plugin, Class<?> clazz);

    /**
     * Unregisters all listeners (reflective and explicit) associated with the plugin.
     *
     * @param plugin The plugin to unregister all listeners for
     */
    void unregisterAllListeners(GrimPlugin plugin);

    /**
     * Unregisters a specific explicit listener associated with the plugin.
     *
     * @param plugin   The plugin unregistering the listener
     * @param listener The explicit listener to unregister
     */
    void unregisterListener(GrimPlugin plugin, GrimEventListener<?> listener);

    /**
     * Posts an event to all registered listeners.
     *
     * @param event The event to post
     */
    void post(@NotNull GrimEvent event);

    /**
     * Subscribes an explicit listener to the specified event type.
     * <p>
     * Example:
     * <pre>{@code
     * eventBus.subscribe(plugin, FlagEvent.class, event -> {
     *     GrimUser user = event.getUser();
     *     // Handle flag
     * }, 1, true);
     * eventBus.subscribe(plugin, GrimJoinEvent.class, this::onJoin);
     * }</pre>
     *
     * @param plugin          The plugin registering the listener
     * @param eventType       The event type to listen for
     * @param listener        The listener to handle the event
     * @param priority        The priority (higher = earlier execution)
     * @param ignoreCancelled Whether to ignore cancelled events
     * @param declaringClass  The class declaring the listener (for unregistration)
     * @param <T>             The event type
     */
    <T extends GrimEvent> void subscribe(GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled, @NotNull Class<?> declaringClass);

    /**
     * Subscribes an explicit listener with default declaring class.
     *
     * @param plugin          The plugin registering the listener
     * @param eventType       The event type to listen for
     * @param listener        The listener to handle the event
     * @param priority        The priority (higher = earlier execution)
     * @param ignoreCancelled Whether to ignore cancelled events
     * @param <T>             The event type
     */
    default <T extends GrimEvent> void subscribe(GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled) {
        subscribe(plugin, eventType, listener, priority, ignoreCancelled, plugin.getClass());
    }

    /**
     * Subscribes an explicit listener with default priority and ignoreCancelled.
     *
     * @param plugin    The plugin registering the listener
     * @param eventType The event type to listen for
     * @param listener  The listener to handle the event
     * @param <T>       The event type
     */
    default <T extends GrimEvent> void subscribe(GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener) {
        subscribe(plugin, eventType, listener, 0, false);
    }
}