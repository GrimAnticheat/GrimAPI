package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Category;
import org.jetbrains.annotations.ApiStatus;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read-only mapping of {@link Category} to the {@link Backend} that hosts it. Built once
 * at startup from a {@code DataStoreConfig} routing table + registered backends.
 */
@ApiStatus.Internal
public final class CategoryRouter {

    private final Map<Category<?>, Backend> routing;
    private final Map<String, Backend> backendsById;

    public CategoryRouter(Map<Category<?>, Backend> routing) {
        this.routing = Map.copyOf(routing);
        LinkedHashMap<String, Backend> byId = new LinkedHashMap<>();
        for (Backend b : routing.values()) byId.putIfAbsent(b.id(), b);
        this.backendsById = Collections.unmodifiableMap(byId);
    }

    public Backend backendFor(Category<?> cat) {
        Backend b = routing.get(cat);
        if (b == null) {
            throw new IllegalArgumentException(
                    "no backend routed for category '" + cat.id() + "'");
        }
        return b;
    }

    public Set<Backend> allBackends() {
        return new LinkedHashSet<>(backendsById.values());
    }

    public Set<Category<?>> routedCategories() {
        return routing.keySet();
    }

    public Map<Category<?>, Backend> routing() {
        return routing;
    }
}
