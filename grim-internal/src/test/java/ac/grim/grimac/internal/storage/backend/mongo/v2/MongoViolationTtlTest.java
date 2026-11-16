package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.event.ViolationEvent;
import ac.grim.grimac.api.storage.kind.EventStream;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.SqlTestDatabases;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Violations used to get a fixed 180-day MongoDB TTL that deleted history whatever the
 * retention config said. New collections get none, the built-in one is switched off, and
 * an operator's own TTL is kept.
 */
@DisplayName("Mongo violations have no built-in expiry")
class MongoViolationTtlTest {

    @Test
    void builtInTtlIsOffAndOperatorTtlStays() throws Exception {
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        BackendV2 backend = SqlTestDatabases.mongo(logs);
        try {
            MongoDatabase db = backend.unwrap(MongoDatabase.class).orElseThrow();
            EventStream<ViolationEvent, ViolationRecord> kind = V2BuiltinKinds.violations();
            KindAdapter<EventStream<ViolationEvent, ViolationRecord>> adapter = backend.adapterFor(kind).orElseThrow();

            String fresh = SqlTestDatabases.uniqueTable("ttl_new");
            adapter.ensureStore(StoreId.grim(fresh), kind);
            assertNull(ttl(db, fresh), "a new collection has no expiry");

            String legacy = SqlTestDatabases.uniqueTable("ttl_legacy");
            create(db, legacy, MongoEventStreamAdapter.LEGACY_BUILTIN_TTL_SECONDS);
            adapter.ensureStore(StoreId.grim(legacy), kind);
            assertNull(ttl(db, legacy), "the built-in 180-day expiry is switched off");

            String operator = SqlTestDatabases.uniqueTable("ttl_operator");
            create(db, operator, TimeUnit.DAYS.toSeconds(30));
            adapter.ensureStore(StoreId.grim(operator), kind);
            assertEquals(TimeUnit.DAYS.toSeconds(30), ttl(db, operator).longValue(), "an operator's expiry is kept");

            for (String c : new String[]{fresh, legacy, operator}) db.getCollection(c).drop();
        } finally {
            backend.close();
        }
    }

    /** When MongoDB refuses to switch the expiry off, the warning says why and how to do it by hand. */
    @Test
    void refusedCollModSaysWhyAndHowToSwitchItOff() throws Exception {
        assertEquals(List.of(
            "[grim-datastore] could not switch off the built-in 180-day expiry on violations_v7: the database user may"
                + " not run collMod (it needs the collMod action, for example from the dbAdmin role). Violations older"
                + " than 180 days keep being deleted by MongoDB until it is switched off, for example by an admin:"
                + " db.runCommand({collMod: \"violations_v7\", expireAfterSeconds: \"off\"})"),
            refusedCollMod(13, "Unauthorized", "not authorized on grim to execute command { collMod: \"violations_v7\" }"));
        List<String> other = refusedCollMod(72, "InvalidOptions", "unknown option");
        assertEquals(1, other.size(), other.toString());
        assertTrue(other.get(0).contains("MongoDB refused collMod (InvalidOptions 72: unknown option)"), other.get(0));
    }

    @SuppressWarnings("unchecked")
    private static List<String> refusedCollMod(int code, String codeName, String message) throws Exception {
        MongoDatabase db = mock(MongoDatabase.class, RETURNS_DEEP_STUBS);
        MongoCursor<String> names = mock(MongoCursor.class);
        when(names.hasNext()).thenReturn(true, false);
        when(names.next()).thenReturn("violations_v7");
        when(db.listCollectionNames().iterator()).thenReturn(names);
        when(db.listCollections().filter(any()).first()).thenReturn(new Document("name", "violations_v7")
            .append("options", new Document("expireAfterSeconds", MongoEventStreamAdapter.LEGACY_BUILTIN_TTL_SECONDS)));
        when(db.runCommand(any(Bson.class))).thenThrow(new MongoCommandException(new BsonDocument("ok", new BsonInt32(0))
            .append("code", new BsonInt32(code)).append("codeName", new BsonString(codeName))
            .append("errmsg", new BsonString(message)), new ServerAddress()));
        SqlTestDatabases.Logs logs = new SqlTestDatabases.Logs();
        new MongoEventStreamAdapter(db, logs.logger(), 64).ensureStore(StoreId.grim("violations_v7"), V2BuiltinKinds.violations());
        return logs.messages(Level.WARNING);
    }

    private static void create(MongoDatabase db, String name, long ttlSeconds) {
        db.createCollection(name, new CreateCollectionOptions()
            .timeSeriesOptions(new TimeSeriesOptions("occurred_at").metaField("meta"))
            .expireAfter(ttlSeconds, TimeUnit.SECONDS));
    }

    private static Number ttl(MongoDatabase db, String name) {
        Document info = db.listCollections().filter(new Document("name", name)).first();
        Object value = ((Document) info.get("options")).get("expireAfterSeconds");
        return value instanceof Number n ? n : null;
    }
}
