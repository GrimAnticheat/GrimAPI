package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.model.VerboseFormat;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoViolationsV6ToV7MigrationTest {

    @Test
    void decodesLegacyTextVerboseString() throws Exception {
        ViolationRecord record = decode(row("offset=-3.8588E-6 .097995 §85", 0));

        assertEquals(VerboseFormat.TEXT, record.verboseFormat());
        assertEquals("offset=-3.8588E-6 .097995 §85", record.verbose());
    }

    @Test
    void decodesLegacyStructuredVerboseBase64String() throws Exception {
        byte[] payload = new byte[] {1, 2, 3, 4, 5};
        ViolationRecord record = decode(row(Base64.getEncoder().encodeToString(payload), 1));

        assertEquals(VerboseFormat.STRUCTURED_V1, record.verboseFormat());
        assertArrayEquals(payload, record.verboseData());
    }

    @SuppressWarnings("unchecked")
    private static ViolationRecord decode(Document document) throws Exception {
        Method method = MongoViolationsV6ToV7Migration.class
            .getDeclaredMethod("decodeV6Doc", Document.class, EventStream.class);
        method.setAccessible(true);
        return (ViolationRecord) method.invoke(null, document, V2BuiltinKinds.violations());
    }

    private static Document row(String verbose, int verboseFormat) {
        return new Document()
            .append("id", BsonBinaries.uuidBinary(UUID.fromString("01800000-0000-7000-8000-000000000001")))
            .append("session_id", BsonBinaries.uuidBinary(UUID.fromString("01800000-0000-7000-8000-000000000002")))
            .append("player_uuid", BsonBinaries.uuidBinary(UUID.fromString("01800000-0000-7000-8000-000000000003")))
            .append("check_id", 1)
            .append("vl", 1.0d)
            .append("occurred_at", 1781956042397L)
            .append("verbose", verbose)
            .append("verbose_format", verboseFormat);
    }
}
