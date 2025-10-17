package ac.grim.grimac.api.event;

import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.NotNull;

/**
 * Event bus for registering and dispatching Grim events.
 * <p>
 * This bus supports both reflective (annotation-based) and explicit (manual) registration of listeners.
 * All registration and unregistration methods require a {@code pluginContext} object. This object is used to
 * manage the listener's lifecycle, automatically unregistering listeners when the associated plugin or extension is disabled.
 * <p>
 * <b>Providing a Plugin Context:</b>
 * <ul>
 *   <li><b>For Platform Plugins (Bukkit, Fabric, etc.):</b> Pass your main plugin/mod instance (e.g., {@code this} from
 *       your {@code JavaPlugin} class). The API will resolve it automatically.</li>
 *   <li><b>For Standalone Extensions:</b> Manually create a {@link GrimPlugin} instance and pass it directly.</li>
 * </ul>
 */
public interface EventBus {

    /**
     * Registers instance methods annotated with {@link GrimEventHandler} as event listeners.
     * This method is provided for convenience and has performance and memory usage overhead.
     * It is recommended you use {@link EventBus#subscribe(Object, Class, GrimEventListener)} instead.
     * <p><b>Example (Bukkit Plugin):</b>
     * <pre>{@code
     * // In your main plugin/mod class that extends JavaPlugin or implements ModContainer
     * eventBus.registerAnnotatedListeners(this, new MyListener());
     * }</pre>
     *
     * @param pluginContext The context (e.g., Bukkit Plugin, Fabric Mod, GrimPlugin) to bind listeners to.
     * @param listener The listener instance containing annotated methods.
     */
    void registerAnnotatedListeners(@NotNull Object pluginContext, @NotNull Object listener);

    /**
     * Registers instance methods annotated with {@link GrimEventHandler} using an explicit {@link GrimPlugin} instance.
     * This is useful for standalone extensions or when you have already resolved a context.
     *
     * @param plugin   The GrimPlugin instance to bind listeners to.
     * @param listener The listener instance containing annotated methods.
     */
    void registerAnnotatedListeners(@NotNull GrimPlugin plugin, @NotNull Object listener);


    /**
     * Registers static methods annotated with {@link GrimEventHandler} as event listeners.
     *
     * <p>
     * Example:
     * <pre>{@code
     * public class MyStaticListener {
     *     @GrimEventHandler(priority = 1, ignoreCancelled = true)
     *     public static void onFlag(FlagEvent event) {
     *         // Handle flag
     *     }
     * }
     * eventBus.registerStaticAnnotatedListeners(this, MyStaticListener.class);
     * }</pre>
     *
     * @param pluginContext The context to bind listeners to.
     * @param clazz  The class containing static annotated methods.
     */
    void registerStaticAnnotatedListeners(@NotNull Object pluginContext, @NotNull Class<?> clazz);

    /**
     * Registers static methods annotated with {@link GrimEventHandler} using an explicit {@link GrimPlugin} instance.
     *
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
     * @param plugin The GrimPlugin instance to bind listeners to.
     * @param clazz  The class containing static annotated methods.
     */
    void registerStaticAnnotatedListeners(@NotNull GrimPlugin plugin, @NotNull Class<?> clazz);

    /**
     * Unregisters all instance listeners associated with the given listener object and its plugin context.
     *
     * @param pluginContext The context the listener was registered with.
     * @param listener The listener instance to unregister.
     */
    void unregisterListeners(@NotNull Object pluginContext, @NotNull Object listener);

    /**
     * Unregisters all instance listeners associated with an explicit {@link GrimPlugin} instance.
     *
     * @param plugin   The GrimPlugin the listener was registered with.
     * @param listener The listener instance to unregister.
     */
    void unregisterListeners(@NotNull GrimPlugin plugin, @NotNull Object listener);


    /**
     * Unregisters all static listeners associated with the given class and its plugin context.
     *
     * @param pluginContext The context the listener was registered with.
     * @param clazz  The class containing static listeners to unregister.
     */
    void unregisterStaticListeners(@NotNull Object pluginContext, @NotNull Class<?> clazz);

    /**
     * Unregisters all static listeners associated with an explicit {@link GrimPlugin} instance.
     *
     * @param plugin The GrimPlugin the listener was registered with.
     * @param clazz  The class containing static listeners to unregister.
     */
    void unregisterStaticListeners(@NotNull GrimPlugin plugin, @NotNull Class<?> clazz);


    /**
     * Unregisters all listeners (reflective and explicit) associated with the given plugin context.
     * This is typically called automatically by GrimAC when a plugin is disabled.
     *
     * @param pluginContext The context to unregister all listeners for.
     */
    void unregisterAllListeners(@NotNull Object pluginContext);

    /**
     * Unregisters all listeners associated with an explicit {@link GrimPlugin} instance.
     *
     * @param plugin The GrimPlugin to unregister all listeners for.
     */
    void unregisterAllListeners(@NotNull GrimPlugin plugin);


    /**
     * Unregisters a specific explicit listener associated with the given plugin context.
     *
     * @param pluginContext The context the listener was registered with.
     * @param listener The explicit listener to unregister.
     */
    void unregisterListener(@NotNull Object pluginContext, @NotNull GrimEventListener<?> listener);

    /**
     * Unregisters a specific explicit listener associated with an explicit {@link GrimPlugin} instance.
     *
     * @param plugin   The GrimPlugin the listener was registered with.
     * @param listener The explicit listener to unregister.
     */
    void unregisterListener(@NotNull GrimPlugin plugin, @NotNull GrimEventListener<?> listener);

    /**
     * Posts an event to all registered listeners.
     *
     * @param event The event to post.
     */
    void post(@NotNull GrimEvent event);

    /**
     * Subscribes an explicit listener to the specified event type.
     * <p><b>Example:</b>
     * <pre>{@code
     * // From a Bukkit Plugin, using 'this' as the context
     * eventBus.subscribe(this, FlagEvent.class, this::onFlag, 1, true, getClass());
     * }</pre>
     *
     * @param pluginContext   The context to bind the listener to.
     * @param eventType       The event type to listen for.
     * @param listener        The listener to handle the event.
     * @param priority        The priority (higher = earlier execution).
     * @param ignoreCancelled Whether to ignore cancelled events.
     * @param declaringClass  The class declaring the listener (for unregistration purposes).
     * @param <T>             The event type.
     */
    <T extends GrimEvent> void subscribe(@NotNull Object pluginContext, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled, @NotNull Class<?> declaringClass);

    /**
     * Subscribes an explicit listener using an explicit {@link GrimPlugin} instance.
     *
     * @param plugin          The GrimPlugin instance to bind the listener to.
     * @param eventType       The event type to listen for.
     * @param listener        The listener to handle the event.
     * @param priority        The priority of the listener.
     * @param ignoreCancelled Whether to ignore cancelled events.
     * @param declaringClass  The class declaring the listener (for unregistration purposes).
     * @param <T>             The event type.
     */
    <T extends GrimEvent> void subscribe(@NotNull GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled, @NotNull Class<?> declaringClass);


    /**
     * Subscribes an explicit listener, using the listener's own class as the declaring class.
     *
     * @param pluginContext   The context to bind the listener to.
     * @param eventType       The event type to listen for.
     * @param listener        The listener to handle the event.
     * @param priority        The priority of the listener.
     * @param ignoreCancelled Whether to ignore cancelled events.
     * @param <T>             The event type.
     */
    default <T extends GrimEvent> void subscribe(@NotNull Object pluginContext, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled) {
        subscribe(pluginContext, eventType, listener, priority, ignoreCancelled, listener.getClass());
    }

    /**
     * Subscribes an explicit listener with a default declaring class.
     *
     * @param plugin          The GrimPlugin instance to bind the listener to.
     * @param eventType       The event type to listen for.
     * @param listener        The listener to handle the event.
     * @param priority        The priority of the listener.
     * @param ignoreCancelled Whether to ignore cancelled events.
     * @param <T>             The event type.
     */
    default <T extends GrimEvent> void subscribe(@NotNull GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener, int priority, boolean ignoreCancelled) {
        subscribe(plugin, eventType, listener, priority, ignoreCancelled, listener.getClass());
    }


    /**
     * Subscribes an explicit listener with default priority (0) and ignoreCancelled (false).
     *
     * @param pluginContext   The context to bind the listener to.
     * @param eventType       The event type to listen for.
     * @param listener        The listener to handle the event.
     * @param <T>             The event type.
     */
    default <T extends GrimEvent> void subscribe(@NotNull Object pluginContext, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener) {
        subscribe(pluginContext, eventType, listener, 0, false);
    }

    /**
     * Subscribes an explicit listener with default priority and ignoreCancelled settings.
     *
     * @param plugin    The GrimPlugin instance to bind the listener to.
     * @param eventType The event type to listen for.
     * @param listener  The listener to handle the event.
     * @param <T>       The event type.
     */
    default <T extends GrimEvent> void subscribe(@NotNull GrimPlugin plugin, @NotNull Class<T> eventType, @NotNull GrimEventListener<T> listener) {
        subscribe(plugin, eventType, listener, 0, false);
    }
}