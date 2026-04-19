package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.backend.BackendContext;
import ac.grim.grimac.api.storage.backend.StorageEventHandler;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.query.DeleteCriteria;
import ac.grim.grimac.api.storage.query.Page;
import ac.grim.grimac.api.storage.query.Query;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CapabilityValidatorTest {

    @Test
    void passesWhenBackendSupportsCategory() {
        InMemoryBackend m = new InMemoryBackend();
        assertDoesNotThrow(() -> CapabilityValidator.validate(Map.of(
                Categories.VIOLATION, m,
                Categories.SESSION, m,
                Categories.SETTING, m)));
    }

    @Test
    void failsWhenBackendDoesNotClaimCategory() {
        Backend noHistory = new RedisLike();
        CapabilityValidator.ValidationException ex = assertThrows(
                CapabilityValidator.ValidationException.class,
                () -> CapabilityValidator.validate(Map.of(Categories.VIOLATION, noHistory)));
        assertTrue(ex.getMessage().contains("does not declare support"));
    }

    @Test
    void failsWhenBackendLacksRequiredCapability() {
        Backend noHistoryCap = new FakeHistoryClaimant();
        CapabilityValidator.ValidationException ex = assertThrows(
                CapabilityValidator.ValidationException.class,
                () -> CapabilityValidator.validate(Map.of(Categories.VIOLATION, noHistoryCap)));
        assertTrue(ex.getMessage().contains("requires capabilities"));
    }

    @Test
    void categoryRouterReturnsBackendPerCategory() {
        InMemoryBackend m = new InMemoryBackend();
        CategoryRouter r = new CategoryRouter(Map.of(
                Categories.VIOLATION, m,
                Categories.SESSION, m));
        assertTrue(r.backendFor(Categories.VIOLATION) == m);
        assertTrue(r.backendFor(Categories.SESSION) == m);
    }

    // Redis-shaped: declares SETTINGS + TTL but NOT HISTORY/INDEXED_KV/TIMESERIES_APPEND.
    // Exercises the "no, you cannot route violations to me" scenario.
    private static final class RedisLike implements Backend {
        @Override public String id() { return "redis-like"; }
        @Override public ApiVersion getApiVersion() { return ApiVersion.CURRENT; }
        @Override public EnumSet<Capability> capabilities() {
            return EnumSet.of(Capability.TTL, Capability.SETTINGS);
        }
        @Override public Set<Category<?>> supportedCategories() {
            return Set.of(Categories.SETTING);
        }
        @Override public void init(BackendContext ctx) {}
        @Override public void flush() {}
        @Override public void close() {}
        @Override public <E> StorageEventHandler<E> eventHandlerFor(Category<E> cat) {
            return (event, seq, endOfBatch) -> {};
        }
        @Override public <R> Page<R> read(Category<?> c, Query<R> q) { return Page.empty(); }
        @Override public <E> void delete(Category<E> c, DeleteCriteria criteria) {}
        @Override public long countViolationsInSession(UUID s) { return 0; }
    }

    // Claims to support HISTORY via supportedCategories but doesn't declare
    // the required capabilities — catches the capability-set check.
    private static final class FakeHistoryClaimant implements Backend {
        @Override public String id() { return "fake-claimant"; }
        @Override public ApiVersion getApiVersion() { return ApiVersion.CURRENT; }
        @Override public EnumSet<Capability> capabilities() {
            return EnumSet.of(Capability.TTL);  // missing INDEXED_KV, TIMESERIES_APPEND, HISTORY
        }
        @Override public Set<Category<?>> supportedCategories() {
            return Set.of(Categories.VIOLATION);
        }
        @Override public void init(BackendContext ctx) {}
        @Override public void flush() {}
        @Override public void close() {}
        @Override public <E> StorageEventHandler<E> eventHandlerFor(Category<E> cat) {
            return (event, seq, endOfBatch) -> {};
        }
        @Override public <R> Page<R> read(Category<?> c, Query<R> q) { return Page.empty(); }
        @Override public <E> void delete(Category<E> c, DeleteCriteria criteria) {}
        @Override public long countViolationsInSession(UUID s) { return 0; }
    }
}
