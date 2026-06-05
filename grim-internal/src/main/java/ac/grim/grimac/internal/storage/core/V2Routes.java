package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.DataKind;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * v2 routing table populated at startup: for each {@link Category} that
 * routes through the new {@link BackendV2} path, holds the destination
 * backend + the typed {@link KindAdapter} + the registered {@link StoreId}.
 * <p>
 * Constructed by whoever wires {@code DataStoreImpl} (today: a small
 * builder in {@code grim-internal}; eventually a proper {@code StoreRegistry}
 * implementation in Phase 3+). Populating is single-threaded at startup;
 * lookup is reader-thread safe because the underlying map is replaced
 * atomically once startup completes.
 */
@ApiStatus.Internal
public final class V2Routes {

    /**
     * Identity-keyed: we deliberately key by Category INSTANCE reference,
     * not by id-equality. Two distinct Category objects with the same id
     * (e.g. legacy {@code Categories.VIOLATION} vs the V2
     * {@code EventStreamCategoryImpl("violation", ...)}) are distinct routes —
     * the v2 path only dispatches when the caller hands us the exact
     * registered instance. Prevents accidental cross-binding.
     */
    private final @NotNull Map<Category<?>, Route<?>> routes;

    private V2Routes(@NotNull IdentityHashMap<Category<?>, Route<?>> routes) {
        this.routes = routes;
    }

    public static @NotNull Builder builder() { return new Builder(); }

    public static @NotNull V2Routes empty() {
        return new V2Routes(new IdentityHashMap<>());
    }

    /**
     * Public lookup so consumers (e.g. {@code HistoryServiceImpl}) can assert
     * their v2 Category instance is actually registered BEFORE switching to the
     * v2 dispatch path. Without this guard, a same-id-but-different-instance
     * Category would fall through to {@code DataStoreImpl.execute}'s "no route"
     * error at first call — a config mismatch caught at runtime instead of boot.
     */
    public boolean contains(@NotNull Category<?> category) {
        return routes.containsKey(category);
    }

    @SuppressWarnings("unchecked")
    public <K extends DataKind<?, ?>> @Nullable Route<K> routeFor(@NotNull Category<?> cat) {
        return (Route<K>) routes.get(cat);
    }

    public boolean isEmpty() { return routes.isEmpty(); }

    public record Route<K extends DataKind<?, ?>>(
            @NotNull StoreId storeId,
            @NotNull BackendV2 backend,
            @NotNull KindAdapter<K> adapter,
            @NotNull K kind) {
    }

    public static final class Builder {
        private final Map<Category<?>, Route<?>> routes = new IdentityHashMap<>();

        @SuppressWarnings({"unchecked", "rawtypes"})
        public <E, R, K extends DataKind<E, R>> @NotNull Builder register(
                @NotNull Category<E> category,
                @NotNull StoreId storeId,
                @NotNull K kind,
                @NotNull BackendV2 backend) {
            if (routes.containsKey(category)) {
                Route<?> existing = routes.get(category);
                throw new IllegalStateException(
                    "duplicate v2 route registration for category " + category.id()
                        + " (existing: storeId=" + existing.storeId() + " kind=" + existing.kind().name()
                        + " backend=" + existing.backend().id() + "; attempted: storeId=" + storeId
                        + " kind=" + kind.name() + " backend=" + backend.id()
                        + ") — two backends claiming the same category is a wiring bug");
            }
            KindAdapter<K> adapter = (KindAdapter<K>) backend.adapterFor(kind).orElseThrow(
                () -> new IllegalArgumentException(
                    "backend " + backend.id() + " does not advertise an adapter for kind " + kind.name()));
            routes.put(category, new Route(storeId, backend, adapter, kind));
            return this;
        }

        public @NotNull V2Routes build() {
            IdentityHashMap<Category<?>, Route<?>> snapshot = new IdentityHashMap<>(routes.size());
            snapshot.putAll(routes);
            return new V2Routes(snapshot);
        }
    }
}
