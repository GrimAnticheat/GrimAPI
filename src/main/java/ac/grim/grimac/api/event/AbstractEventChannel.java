package ac.grim.grimac.api.event;

import ac.grim.grimac.api.plugin.GrimPlugin;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Coordinator for subscribing to an <em>abstract</em> event type that has
 * multiple concrete subtypes — e.g. {@link ac.grim.grimac.api.event.events.GrimCheckEvent}
 * dispatches through {@code FlagEvent}, {@code CompletePredictionEvent}, and
 * {@code CommandExecuteEvent}.
 *
 * <p>The implementation uses the <b>bridge-listener</b> pattern: an
 * abstract subscribe installs one concrete-typed <i>bridge</i> entry into
 * every registered child channel. The hot path ({@code child.fire(...)})
 * iterates its entries as normal — bridges are indistinguishable from
 * direct typed subscribers for sort order, priority, and cancellation. No
 * extra dispatch logic on fire.
 *
 * <p>Late-registered subtypes are covered: {@link #registerSubtype} walks
 * the abstract-subscriber list and installs bridges on the newly-added
 * child channel. Unsubscribing an abstract handler sweeps every bridge it
 * installed across all concrete channels.
 *
 * <h2>Addon authors</h2>
 * A subclass of an abstract event (e.g. a custom check type extending
 * {@code GrimCheckEvent}) must opt in so it receives abstract-level
 * dispatches. After {@code bus.register(MyEvent.class, myChannel)}:
 *
 * <pre>{@code
 * GrimCheckEvent.Channel abs = (GrimCheckEvent.Channel) bus.get(GrimCheckEvent.class);
 * abs.registerSubtype(MyEvent.class, myChannel, MyEvent.Channel::bridgeFromCheck);
 * }</pre>
 *
 * @param <E> the abstract event type
 * @param <H> the abstract handler type
 */
public abstract class AbstractEventChannel<E extends GrimEvent<?>, H> extends EventChannel<E, H> {

    /** Live subscribers at this abstract level. Never iterated on the hot path. */
    private final List<AbstractSub<H>> abstractSubs = new CopyOnWriteArrayList<>();

    /** Registered concrete subtype → (child channel, bridge factory). */
    private final Map<Class<? extends E>, ChildEntry<H, ?>> children = new ConcurrentHashMap<>();

    protected AbstractEventChannel(@NotNull Class<E> eventClass, @NotNull Class<H> handlerType) {
        super(eventClass, handlerType);
    }

    /**
     * Register a concrete subtype with this abstract channel so any current
     * and future abstract subscribers receive bridged dispatches when the
     * concrete channel fires. Safe to call either before or after abstract
     * subscribers are added — {@code registerSubtype} installs bridges for
     * every subscriber already on the list.
     *
     * @param concreteType    the subtype's {@code Class}
     * @param concreteChannel the subtype's channel
     * @param bridgeFactory   converts an abstract {@code H} handler into a
     *                        concrete handler that, when invoked by the
     *                        concrete channel's fire, calls back into the
     *                        abstract handler with the shared fields
     */
    @ApiStatus.Internal
    public <CH> void registerSubtype(@NotNull Class<? extends E> concreteType,
                                     @NotNull EventChannel<?, CH> concreteChannel,
                                     @NotNull Function<H, CH> bridgeFactory) {
        ChildEntry<H, CH> entry = new ChildEntry<>(concreteChannel, bridgeFactory);
        children.put(concreteType, entry);
        for (AbstractSub<H> sub : abstractSubs) {
            installBridge(sub, entry);
        }
    }

    /**
     * Protected entry point used by subclass {@code on…(…)} methods — adds
     * the handler to the abstract subscriber list and installs a bridge
     * entry on every currently-registered concrete child channel.
     */
    protected final void subscribeAbstract(@NotNull H handler, int priority, boolean ignoreCancelled,
                                           @Nullable Object pluginContext) {
        AbstractSub<H> sub = new AbstractSub<>(handler, pluginContext, priority, ignoreCancelled);
        abstractSubs.add(sub);
        for (ChildEntry<H, ?> entry : children.values()) {
            installBridge(sub, entry);
        }
    }

    /**
     * Resolves a platform-specific context ({@link Object}) into a
     * {@link GrimPlugin} using the resolver installed by the bus, then
     * delegates to the plugin-bound overload. Matches the behaviour of
     * {@link EventChannel#resolvePlugin(Object)} on concrete channels.
     */
    protected final void subscribeAbstractResolving(@NotNull Object pluginContext, @NotNull H handler,
                                                    int priority, boolean ignoreCancelled) {
        subscribeAbstract(handler, priority, ignoreCancelled, resolvePlugin(pluginContext));
    }

    /**
     * Removes the given abstract handler and sweeps every bridge it
     * installed across all concrete child channels.
     */
    @Override
    public void unsubscribe(@NotNull H handler) {
        AbstractSub<H> target = null;
        synchronized (abstractSubs) {
            for (AbstractSub<H> sub : abstractSubs) {
                if (sub.handler == handler) { target = sub; break; }
            }
            if (target == null) return;
            abstractSubs.remove(target);
        }
        for (Bridge b : target.bridges) b.sweep();
    }

    /**
     * Unreachable — an abstract channel holds no direct entries, so
     * {@link EventChannel#dispatchLegacy(GrimEvent)} on it always iterates
     * an empty array. The legacy dispatch reaches subscribers via the
     * bridges installed in each concrete child channel.
     */
    @Override
    protected final boolean dispatchTypedFromLegacy(@NotNull E event, @NotNull H handler, boolean cancelled) {
        throw new UnsupportedOperationException(
                "AbstractEventChannel has no direct entries — bridges dispatch through concrete children");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <CH> void installBridge(AbstractSub<H> sub, ChildEntry<H, CH> entry) {
        CH bridgeHandler = entry.bridgeFactory.apply(sub.handler);
        ((EventChannel) entry.channel).subscribeTyped(bridgeHandler, sub.priority, sub.ignoreCancelled, sub.pluginContext);
        sub.bridges.add(new Bridge(entry.channel, bridgeHandler));
    }

    private static final class AbstractSub<H> {
        final H handler;
        final @Nullable Object pluginContext;
        final int priority;
        final boolean ignoreCancelled;
        final List<Bridge> bridges = new ArrayList<>();

        AbstractSub(H handler, @Nullable Object pluginContext, int priority, boolean ignoreCancelled) {
            this.handler = handler;
            this.pluginContext = pluginContext;
            this.priority = priority;
            this.ignoreCancelled = ignoreCancelled;
        }
    }

    private static final class Bridge {
        final EventChannel<?, ?> channel;
        final Object handler;

        Bridge(EventChannel<?, ?> channel, Object handler) {
            this.channel = channel;
            this.handler = handler;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void sweep() {
            ((EventChannel) channel).unsubscribe(handler);
        }
    }

    private static final class ChildEntry<H, CH> {
        final EventChannel<?, CH> channel;
        final Function<H, CH> bridgeFactory;

        ChildEntry(EventChannel<?, CH> channel, Function<H, CH> bridgeFactory) {
            this.channel = channel;
            this.bridgeFactory = bridgeFactory;
        }
    }
}
