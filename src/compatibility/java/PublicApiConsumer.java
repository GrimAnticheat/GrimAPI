import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.TransferData;
import ac.grim.grimac.api.storage.backend.ApiVersion;
import ac.grim.grimac.api.storage.category.AccessPattern;
import ac.grim.grimac.api.storage.category.Capability;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.event.BlobEvent;
import ac.grim.grimac.api.storage.event.SessionEvent;
import ac.grim.grimac.api.storage.extension.BlobStore;
import ac.grim.grimac.api.storage.extension.ExtensionStorageCategories;
import ac.grim.grimac.api.storage.extension.ExtensionStorageEvent;
import ac.grim.grimac.api.storage.extension.ExtensionStorageQueries;
import ac.grim.grimac.api.storage.model.BlobRef;
import ac.grim.grimac.api.storage.model.ExtensionStorageRecord;
import ac.grim.grimac.api.storage.model.SessionBlobRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

/** Compiled against the public jar once, then run unchanged with a selected API jar. */
public final class PublicApiConsumer {
    public static void main(String[] args) throws Exception {
        TransferData transfer = TransferData.builder().position(1, 2, 3).velocity(4, 5, 6).rotation(90, -20).build();
        Object[] received = new Object[1];
        GrimUser user = (GrimUser) Proxy.newProxyInstance(GrimUser.class.getClassLoader(), new Class<?>[]{GrimUser.class},
                (proxy, method, values) -> { received[0] = values[0]; return null; });
        user.setTransferData(transfer);
        equal(transfer, received[0]);
        equal(6.0, transfer.velocityZ());
        equal(90.0f, transfer.yaw());
        equal(true, transfer.hasRotation());
        equal(new ApiVersion(2, 0), ApiVersion.V2_0);
        equal(ApiVersion.V2_0, ApiVersion.CURRENT);
        equal(Capability.EXTENSION_STORAGE, Capability.valueOf("EXTENSION_STORAGE"));

        UUID session = UUID.randomUUID();
        UUID player = UUID.randomUUID();
        BlobRef ref = new BlobRef("memory", "demo/key", 3, "application/octet-stream");
        BlobEvent slot = Categories.BLOB.newEvent().get();
        SessionBlobRecord blob = slot.sessionId(session).playerUuid(player).blobRef(ref).kind("demo")
                .codec("bytes").startOffsetMs(5).durationMs(10).label("test").metadata("{}").truncated(true).toRecord();
        equal(SessionBlobRecord.class, Categories.BLOB.queryResultType());
        equal(ref, blob.blobRef());
        SessionEvent event = new SessionEvent().sessionId(session).playerUuid(player).addSessionBlob(blob);
        equal(List.of(blob), event.sessionBlobs());
        event.replaceSessionBlobs(List.of(blob));
        equal(1, event.sessionBlobs().size());
        event.reset();
        equal(List.of(), event.sessionBlobs());
        slot.reset();
        equal(null, slot.blobRef());

        List<SessionBlobRecord> blobs = List.of(blob);
        SessionRecord canonical = new SessionRecord(session, player, "test", 1, 2, 0L,
                "test", "vanilla", 767, "test", null, null, blobs);
        SessionRecord previous = new SessionRecord(session, player, "test", 1, 2, 0L,
                "test", "vanilla", 767, "test", null, blobs);
        SessionRecord legacy = new SessionRecord(session, player, "test", 1, 2, (Long) null,
                "test", "vanilla", 767, "test", blobs);
        for (SessionRecord record : List.of(canonical, previous, legacy)) {
            equal(blobs, record.sessionBlobs());
            equal(false, record.isClosed());
            try { record.sessionBlobs().clear(); throw new AssertionError("mutable record list"); }
            catch (UnsupportedOperationException expected) { }
        }
        ViolationRecord violation = new ViolationRecord(UUID.randomUUID(), session, player, 1, 2, 3,
                "hello", VerboseFormat.TEXT);
        equal("hello", violation.verbose());

        var category = ExtensionStorageCategories.binary("Demo Plugin", "records", AccessPattern.INDEXED_KV,
                EnumSet.noneOf(Capability.class));
        equal("ext/demo_plugin/records", category.id());
        equal(true, category.requiredCapabilities().contains(Capability.EXTENSION_STORAGE));
        equal(ExtensionStorageEvent.class, category.newEvent().get().getClass());
        equal("key", new ExtensionStorageQueries.GetLatest("key").key());
        byte[] bytes = {1, 2, 3};
        ExtensionStorageRecord record = new ExtensionStorageRecord(category.id(), "key", bytes, 1, 2, "{}");
        bytes[0] = 9;
        equal((byte) 1, record.value()[0]);
        record.value()[0] = 8;
        equal((byte) 1, record.value()[0]);

        MemoryBlobs storage = new MemoryBlobs();
        BlobStore.BlobPutOptions options = new BlobStore.BlobPutOptions("text/plain", 3, Map.of());
        BlobRef stored = storage.put("demo", output -> output.write("abc".getBytes(StandardCharsets.UTF_8)), options);
        equal("demo/key", stored.key());
        equal(3L, storage.size("demo", "key"));
        equal(true, storage.has("demo", "key"));
        try (InputStream input = storage.get("demo", "key")) {
            equal("abc", new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
        storage.delete("demo", "key");
        equal(false, storage.has("demo", "key"));
        System.out.println("Public API consumer passed with " + args[0]);
    }

    private static void equal(Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) throw new AssertionError("Expected " + expected + ", got " + actual);
    }

    private static final class MemoryBlobs implements BlobStore {
        private byte[] value;
        @Override public String backendId() { return "memory"; }
        @Override public BlobRef put(String namespace, InputStream input, BlobPutOptions options) throws IOException {
            value = input.readAllBytes();
            return new BlobRef(backendId(), namespace + "/key", value.length, options.contentType());
        }
        @Override public BlobRef put(String namespace, OutputStreamConsumer producer, BlobPutOptions options) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            producer.writeTo(output);
            return put(namespace, new ByteArrayInputStream(output.toByteArray()), options);
        }
        @Override public InputStream get(BlobRef ref) { return new ByteArrayInputStream(value); }
        @Override public long size(BlobRef ref) { return value.length; }
        @Override public boolean has(BlobRef ref) { return value != null; }
        @Override public void delete(BlobRef ref) { value = null; }
        @Override public Stream<BlobRef> list(String namespace, String prefix) { return Stream.empty(); }
    }
}
