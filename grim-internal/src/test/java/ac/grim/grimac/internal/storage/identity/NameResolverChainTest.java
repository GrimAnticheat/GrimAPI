package ac.grim.grimac.internal.storage.identity;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.identity.NameResolverLink;
import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NameResolverChainTest {

    private InMemoryBackend backend;
    private DataStoreImpl store;

    @BeforeEach
    void setup() {
        backend = new InMemoryBackend();
        Map<Category<?>, Backend> routing = Map.of(
                Categories.PLAYER_IDENTITY, backend,
                Categories.SESSION, backend,
                Categories.VIOLATION, backend,
                Categories.SETTING, backend);
        store = new DataStoreImpl(new CategoryRouter(routing), WritePathConfig.defaults(), Logger.getLogger("nrc"));
        store.start();
    }

    @AfterEach
    void teardown() {
        store.flushAndClose(500);
    }

    @Test
    void localCacheLinkResolvesObservedIdentity() throws Exception {
        UUID player = UUID.randomUUID();
        backend.writeRecordsDirect(Categories.PLAYER_IDENTITY, List.of(new PlayerIdentity(player, "Alice", 1, 2)));
        LocalCacheLink link = new LocalCacheLink(store);
        Optional<UUID> byName = link.resolveByName("Alice").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of(player), byName);
        Optional<String> byUuid = link.resolveByUuid(player).toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of("Alice"), byUuid);
    }

    @Test
    void chainStopsAtFirstNonEmpty() throws Exception {
        AtomicInteger calledSecond = new AtomicInteger();
        NameResolverLink first = new AlwaysHit();
        NameResolverLink second = new NameResolverLink() {
            @Override public String id() { return "second"; }
            @Override public CompletionStage<Optional<UUID>> resolveByName(String name) {
                calledSecond.incrementAndGet();
                return CompletableFuture.completedStage(Optional.empty());
            }
            @Override public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
                return CompletableFuture.completedStage(Optional.empty());
            }
        };
        NameResolverChain chain = new NameResolverChain(List.of(first, second));
        Optional<UUID> res = chain.resolveByName("Anything").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(res.isPresent());
        assertEquals(0, calledSecond.get(), "second link should not be consulted");
    }

    @Test
    void chainFallsThroughToLaterLink() throws Exception {
        NameResolverLink miss = new NameResolverLink() {
            @Override public String id() { return "miss"; }
            @Override public CompletionStage<Optional<UUID>> resolveByName(String name) {
                return CompletableFuture.completedStage(Optional.empty());
            }
            @Override public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
                return CompletableFuture.completedStage(Optional.empty());
            }
        };
        NameResolverChain chain = new NameResolverChain(List.of(miss, new OfflineModeUuidLink()));
        Optional<UUID> res = chain.resolveByName("Alice").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(res.isPresent(), "OfflineModeUuidLink always returns a derived UUID");
    }

    @Test
    void offlineModeLinkDerivesExpectedUuidFormat() throws Exception {
        OfflineModeUuidLink link = new OfflineModeUuidLink();
        UUID a = link.resolveByName("Notch").toCompletableFuture().get().orElseThrow();
        UUID b = link.resolveByName("Notch").toCompletableFuture().get().orElseThrow();
        assertEquals(a, b, "deterministic");
        Optional<String> reverse = link.resolveByUuid(a).toCompletableFuture().get();
        assertTrue(reverse.isEmpty(), "offline-mode link cannot resolve uuid -> name");
    }

    @Test
    void playerIdentityServiceWritesOnObserve() throws Exception {
        PlayerIdentityService svc = new PlayerIdentityService(store);
        UUID player = UUID.randomUUID();
        svc.observe(player, "Alice", 1000L);
        // Poll the backend via the resolver — queue-size hitting 0 precedes the actual
        // writeBatch completing, so wait for the observable effect.
        LocalCacheLink link = new LocalCacheLink(store);
        Optional<UUID> resolved = Optional.empty();
        long deadline = System.currentTimeMillis() + 3_000;
        while (resolved.isEmpty() && System.currentTimeMillis() < deadline) {
            resolved = link.resolveByName("Alice").toCompletableFuture().get(1, TimeUnit.SECONDS);
            if (resolved.isEmpty()) Thread.sleep(20);
        }
        assertEquals(Optional.of(player), resolved);
    }

    private void awaitEmpty() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (store.metrics().queuedCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    private static final class AlwaysHit implements NameResolverLink {
        @Override public String id() { return "always"; }
        @Override public CompletionStage<Optional<UUID>> resolveByName(String name) {
            return CompletableFuture.completedStage(Optional.of(new UUID(1, 2)));
        }
        @Override public CompletionStage<Optional<String>> resolveByUuid(UUID uuid) {
            return CompletableFuture.completedStage(Optional.of("hit"));
        }
    }
}
