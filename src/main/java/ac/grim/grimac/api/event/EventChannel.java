package ac.grim.grimac.api.event;

import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Per-event subscriber registry and dispatch primitive.
 *
 * <p>Every event defines its own {@code EventChannel} subclass that adds a
 * typed {@code fire(...)} method taking the event's parameters directly.
 * Dispatch is zero-allocation on the hot path: the volatile subscriber array
 * is pre-sorted by ascending priority (lower fires first, higher gets the
 * final say on cancellation — Bukkit {@code EventPriority} convention), the
 * typed handler is invoked through a SAM interface without wrapping the call
 * in a {@code GrimEvent} object, and the legacy pooled-event pathway is
 * skipped entirely when no legacy subscriber is attached.
 *
 * <h2>Subscriber kinds</h2>
 * A channel stores a single copy-on-write {@link Entry} array. Each entry is
 * either:
 * <ul>
 *   <li><b>typed</b> — holds an {@code H} handler. Dispatched by calling the
 *       handler directly with primitive/reference parameters.</li>
 *   <li><b>legacy</b> — holds a {@link GrimEventListener} bound to the event's
 *       {@code GrimEvent} subclass. Dispatched by populating a per-thread
 *       pooled event instance and invoking the listener.</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * {@code subscribe} / {@code unsubscribe} mutations swap the array under a
 * monitor and are safe to call concurrently with {@link #entries()} and with
 * in-flight {@code fire(...)} calls from other threads. The array itself is
 * never mutated after publication.
 *
 * <h2>Cancellation</h2>
 * The base class is cancellation-agnostic. Subclasses for cancellable events
 * thread a {@code boolean cancelled} through their dispatch loop themselves;
 * non-cancellable channels simply don't have that parameter.
 *
 * @param <E> the event class this channel dispatches
 * @param <H> the typed handler interface for this event
 */
public abstract class EventChannel<E extends GrimEvent<?>, H> {
    private final Class<E> eventClass;
    private final Class<H> handlerType;
    private final Object writeLock = new Object();
    private volatile Entry<H>[] entries;
    private volatile int legacyCount;
    private volatile @Nullable PluginResolver pluginResolver;

    @SuppressWarnings("unchecked")
    protected EventChannel(@NotNull Class<E> eventClass, @NotNull Class<H> handlerType) {
        this.eventClass = eventClass;
        this.handlerType = handlerType;
        this.entries = (Entry<H>[]) new Entry[0];
    }

    /** Returns the event class this channel serves. */
    public final @NotNull Class<E> eventClass() {
        return eventClass;
    }

    /** Returns the typed handler interface for this channel. */
    public final @NotNull Class<H> handlerType() {
        return handlerType;
    }

    /**
     * Snapshot of the sorted subscriber array. The returned array is never
     * mutated after publication, so iteration is safe to use on the dispatch
     * hot path without copying.
     */
    protected final @NotNull Entry<H>[] entries() {
        return entries;
    }

    /**
     * Returns {@code true} if any legacy subscriber is currently attached.
     * Dispatch uses this to decide whether to touch its pooled-event
     * {@code ThreadLocal} — skipping it entirely when the answer is {@code false}.
     */
    public final boolean hasLegacy() {
        return legacyCount != 0;
    }

    /** Returns {@code true} if no subscribers of any kind are registered. */
    public final boolean isEmpty() {
        return entries.length == 0;
    }

    /**
     * Registers a typed handler. Intended as the building block for the
     * subclass's fluent {@code on…(…)} methods — external callers should use
     * those, and plugin-lifecycle-bound subscribes should go through
     * {@link EventBus#get(Class)} + the on-method so the bus can clean up
     * automatically on plugin disable.
     */
    protected final void subscribe(@NotNull H handler, int priority, boolean ignoreCancelled,
                                   @Nullable Object pluginContext, @Nullable Class<?> declaringClass) {
        addEntry(new Entry<>(handler, null, null, priority, ignoreCancelled, pluginContext, declaringClass));
    }

    /**
     * Internal: used by {@link AbstractEventChannel} to install a bridge
     * entry into this concrete channel. Equivalent to the protected
     * {@link #subscribe(Object, int, boolean, Object, Class)} with
     * {@code declaringClass = null}, but publicly callable so the abstract
     * channel (which lives in the same package) can reach cross-instance.
     */
    @ApiStatus.Internal
    public final void subscribeTyped(@NotNull H handler, int priority, boolean ignoreCancelled,
                                     @Nullable Object pluginContext) {
        subscribe(handler, priority, ignoreCancelled, pluginContext, null);
    }

    /**
     * Resolves a platform-specific plugin context ({@code JavaPlugin},
     * Fabric {@code ModContainer}, …) into a {@link GrimPlugin}. Used by the
     * deprecated {@code onX(Object ctx, Handler)} overloads on each
     * subclass so plugin authors that don't have a {@code GrimPlugin} handy
     * can still subscribe with lifecycle tracking.
     *
     * <p>Requires the bus to have installed a resolver via
     * {@link #setPluginResolver(PluginResolver)}; channels used outside a
     * bus (e.g. in unit tests) throw {@link IllegalStateException}.
     */
    @ApiStatus.Internal
    protected final @NotNull GrimPlugin resolvePlugin(@NotNull Object pluginContext) {
        if (pluginContext instanceof GrimPlugin) return (GrimPlugin) pluginContext;
        PluginResolver r = this.pluginResolver;
        if (r == null) {
            throw new IllegalStateException("EventChannel " + eventClass.getSimpleName()
                    + " has no plugin resolver installed; pass a GrimPlugin directly instead of a platform-specific context.");
        }
        return r.resolve(pluginContext);
    }

    /**
     * Internal: installed by the bus on every registered channel so the
     * deprecated Object-taking {@code onX(...)} overloads can route to a
     * {@link GrimPlugin}.
     */
    @ApiStatus.Internal
    public final void setPluginResolver(@Nullable PluginResolver resolver) {
        this.pluginResolver = resolver;
    }

    /** Strategy for {@link #resolvePlugin(Object)}. Bus-provided. */
    @FunctionalInterface
    public interface PluginResolver {
        @NotNull GrimPlugin resolve(@NotNull Object pluginContext);
    }

    /**
     * Removes a previously registered typed handler. A no-op if the handler
     * was never subscribed.
     */
    public void unsubscribe(@NotNull H handler) {
        removeMatching(e -> e.handler == handler);
    }

    /**
     * Internal: registers a legacy {@link GrimEventListener} on this channel's
     * legacy slot. Invoked by the bus when a caller uses the deprecated
     * class-keyed subscribe to bridge into the channel.
     */
    @ApiStatus.Internal
    public final <GE extends GrimEvent<?>> void subscribeLegacy(
            @NotNull GrimEventListener<GE> listener,
            @NotNull Class<GE> legacyEventClass,
            int priority, boolean ignoreCancelled,
            @Nullable Object pluginContext, @Nullable Class<?> declaringClass) {
        addEntry(new Entry<>(null, listener, legacyEventClass, priority, ignoreCancelled, pluginContext, declaringClass));
    }

    /** Internal: removes a specific legacy listener. */
    @ApiStatus.Internal
    public final void unsubscribeLegacy(@NotNull GrimEventListener<?> listener) {
        removeMatching(e -> e.legacyListener == listener);
    }

    /** Internal: removes every subscriber bound to the given plugin context. */
    @ApiStatus.Internal
    public final void unsubscribeAllFromPlugin(@NotNull Object pluginContext) {
        removeMatching(e -> pluginContext.equals(e.pluginContext));
    }

    /** Internal: removes every legacy subscriber declared by the given class. */
    @ApiStatus.Internal
    public final void unsubscribeLegacyFromClass(@NotNull Object pluginContext, @NotNull Class<?> declaringClass) {
        removeMatching(e -> e.legacyListener != null
                && pluginContext.equals(e.pluginContext)
                && declaringClass.equals(e.declaringClass));
    }

    /**
     * Internal: dispatches a legacy-style {@code GrimEvent} through this
     * channel's subscribers. Both legacy-registered listeners and typed
     * handlers are invoked — typed handlers by unpacking the event's fields
     * via {@link #dispatchTypedFromLegacy(GrimEvent, Object, boolean)}.
     *
     * <p>Only reachable through {@link EventBus#post(GrimEvent)}. Internal
     * Grim firings should call the subclass's typed {@code fire(...)}
     * directly — it has the same dispatch semantics without the caller-side
     * event instance allocation.
     */
    @ApiStatus.Internal
    public final void dispatchLegacy(@NotNull E event) {
        Entry<H>[] arr = entries;
        if (arr.length == 0) return;

        final boolean isCan = event instanceof Cancellable;
        boolean cancelled = isCan && ((Cancellable) event).isCancelled();

        for (Entry<H> e : arr) {
            if (cancelled && !e.ignoreCancelled) continue;
            try {
                if (e.legacyListener != null) {
                    if (isCan) ((Cancellable) event).setCancelled(cancelled);
                    @SuppressWarnings("unchecked")
                    GrimEventListener<E> listener = (GrimEventListener<E>) e.legacyListener;
                    listener.handle(event);
                    if (isCan) cancelled = ((Cancellable) event).isCancelled();
                } else {
                    cancelled = dispatchTypedFromLegacy(event, e.handler, cancelled);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    /**
     * Subclass hook: unpack the fields of a legacy event instance and invoke
     * the typed {@code H} handler with positional parameters. Return the new
     * cancelled state; non-cancellable channels ignore the parameter and
     * return {@code false}.
     *
     * <p>Only invoked via {@link #dispatchLegacy(GrimEvent)}. The hot-path
     * typed {@code fire(...)} never takes this route.
     */
    protected abstract boolean dispatchTypedFromLegacy(@NotNull E event, @NotNull H handler, boolean cancelled);

    @SuppressWarnings("unchecked")
    private void addEntry(Entry<H> entry) {
        synchronized (writeLock) {
            Entry<H>[] old = entries;
            Entry<H>[] next = Arrays.copyOf(old, old.length + 1);
            // Ascending priority: lower priority fires first, higher gets the final
            // say on cancellation. Matches Bukkit / Paper EventPriority ordering.
            int i = old.length;
            while (i > 0 && old[i - 1].priority > entry.priority) {
                next[i] = old[i - 1];
                i--;
            }
            next[i] = entry;
            entries = next;
            if (entry.legacyListener != null) legacyCount++;
        }
    }

    @SuppressWarnings("unchecked")
    private void removeMatching(@NotNull java.util.function.Predicate<Entry<H>> filter) {
        synchronized (writeLock) {
            Entry<H>[] old = entries;
            int keep = 0;
            for (Entry<H> e : old) if (!filter.test(e)) keep++;
            if (keep == old.length) return;
            Entry<H>[] next = (Entry<H>[]) new Entry[keep];
            int removedLegacy = 0;
            int idx = 0;
            for (Entry<H> e : old) {
                if (filter.test(e)) {
                    if (e.legacyListener != null) removedLegacy++;
                } else {
                    next[idx++] = e;
                }
            }
            entries = next;
            legacyCount -= removedLegacy;
        }
    }

    /**
     * One registered subscriber. Either {@code handler} is set (typed entry)
     * or {@code legacyListener} + {@code legacyEventClass} are set (legacy
     * entry) — never both. Entries are otherwise immutable after publication.
     */
    public static final class Entry<H> {
        public final @Nullable H handler;
        public final @Nullable GrimEventListener<? extends GrimEvent<?>> legacyListener;
        public final @Nullable Class<? extends GrimEvent<?>> legacyEventClass;
        public final int priority;
        public final boolean ignoreCancelled;
        public final @Nullable Object pluginContext;
        public final @Nullable Class<?> declaringClass;

        Entry(@Nullable H handler,
              @Nullable GrimEventListener<? extends GrimEvent<?>> legacyListener,
              @Nullable Class<? extends GrimEvent<?>> legacyEventClass,
              int priority, boolean ignoreCancelled,
              @Nullable Object pluginContext, @Nullable Class<?> declaringClass) {
            this.handler = handler;
            this.legacyListener = legacyListener;
            this.legacyEventClass = legacyEventClass;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
            this.pluginContext = pluginContext;
            this.declaringClass = declaringClass;
        }

        public boolean isLegacy() {
            return legacyListener != null;
        }

        @SuppressWarnings("unchecked")
        public <GE extends GrimEvent<?>> @NotNull GrimEventListener<GE> legacyListenerAs() {
            return (GrimEventListener<GE>) legacyListener;
        }
    }
}
