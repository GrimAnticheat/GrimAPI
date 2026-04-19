package ac.grim.grimac.internal.storage.submit;

import ac.grim.grimac.api.storage.backend.Backend;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.WritePathConfig;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.submit.SubmitResult;
import ac.grim.grimac.internal.storage.backend.memory.InMemoryBackend;
import ac.grim.grimac.internal.storage.core.CategoryRouter;
import ac.grim.grimac.internal.storage.core.DataStoreImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ViolationSinkImplTest {

    private InMemoryBackend backend;
    private DataStoreImpl store;
    private ViolationSinkImpl sink;

    @BeforeEach
    void setup() {
        backend = new InMemoryBackend();
        Map<Category<?>, Backend> routing = Map.of(
                Categories.VIOLATION, backend,
                Categories.SESSION, backend,
                Categories.PLAYER_IDENTITY, backend,
                Categories.SETTING, backend);
        store = new DataStoreImpl(new CategoryRouter(routing), WritePathConfig.defaults(), Logger.getLogger("vs"));
        store.start();
        sink = new ViolationSinkImpl(store);
    }

    @AfterEach
    void teardown() {
        store.flushAndClose(500);
    }

    @Test
    void recordReturnsQueuedWhenOpen() {
        SubmitResult r = sink.record(configurer());
        assertEquals(SubmitResult.QUEUED, r);
    }

    @Test
    void recordReturnsShuttingDownAfterShutDown() {
        sink.shutDown();
        SubmitResult r = sink.record(configurer());
        assertEquals(SubmitResult.DROPPED_SHUTTING_DOWN, r);
    }

    private Consumer<ViolationEvent> configurer() {
        return e -> e
                .sessionId(UUID.randomUUID())
                .playerUuid(UUID.randomUUID())
                .checkId(1)
                .vl(1.0)
                .occurredEpochMs(System.currentTimeMillis())
                .verbose("v")
                .verboseFormat(VerboseFormat.TEXT);
    }
}
