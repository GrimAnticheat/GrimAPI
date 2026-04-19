package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendConfig;
import ac.grim.grimac.api.storage.backend.BackendProvider;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackendProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BackendRegistryImplTest {

    @Test
    void registeredProviderIsLookable() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        InMemoryBackendProvider p = new InMemoryBackendProvider();
        registry.register(p);
        assertSame(p, registry.lookup("memory"));
        assertTrue(registry.registeredIds().contains("memory"));
    }

    @Test
    void unknownIdLookupReturnsNull() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        assertNull(registry.lookup("mysql"));
    }

    @Test
    void duplicateIdReplacesPrevious() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        BackendProvider first = new FakeProvider("mysql");
        BackendProvider second = new FakeProvider("mysql");
        registry.register(first);
        registry.register(second);
        assertSame(second, registry.lookup("mysql"));
        assertEquals(1, registry.registeredIds().size());
    }

    @Test
    void blankIdRejected() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        assertThrows(IllegalArgumentException.class, () -> registry.register(new FakeProvider("")));
        assertThrows(IllegalArgumentException.class, () -> registry.register(new FakeProvider("   ")));
    }

    @Test
    void registeredIdsSnapshotIsImmutable() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        registry.register(new InMemoryBackendProvider());
        var snapshot = registry.registeredIds();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add("other"));
    }

    @Test
    void providerBuildsBackendFromConfig() {
        BackendRegistryImpl registry = new BackendRegistryImpl();
        registry.register(new InMemoryBackendProvider());
        BackendProvider provider = registry.lookup("memory");
        assertNotNull(provider);
        Backend backend = provider.create(new InMemoryBackend.Config());
        assertInstanceOf(InMemoryBackend.class, backend);
    }

    private record FakeProvider(String id) implements BackendProvider {
        @Override public @NotNull String id() { return id; }
        @Override public @NotNull Class<? extends BackendConfig> configType() { return BackendConfig.class; }
        @Override public @NotNull Backend create(@NotNull BackendConfig config) {
            throw new UnsupportedOperationException();
        }
    }
}
