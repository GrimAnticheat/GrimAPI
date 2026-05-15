package ac.grim.grimac.internal.storage.backend.mongo;

import ac.grim.grimac.api.storage.check.CheckCatalogPersistence;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

@ApiStatus.Internal
final class MongoCheckCatalogPersistence implements CheckCatalogPersistence {

    private final MongoCollection<Document> meta;
    private final MongoCollection<Document> checks;

    MongoCheckCatalogPersistence(MongoCollection<Document> meta, MongoCollection<Document> checks) {
        this.meta = meta;
        this.checks = checks;
    }

    @Override
    public Iterable<CheckCatalogRow> loadAll() {
        List<CheckCatalogRow> out = new ArrayList<>();
        for (Document d : checks.find()) {
            CheckCatalogRow row = toRow(d);
            if (row != null) out.add(row);
        }
        return out;
    }

    @Override
    public int insert(String stableKey,
                      @Nullable String display,
                      @Nullable String description,
                      @Nullable String introducedVersion,
                      long introducedAt) {
        int id = nextCheckId();
        Document d = new Document("_id", stableKey)
                .append("check_id", id)
                .append("stable_key", stableKey)
                .append("display", display)
                .append("description", description)
                .append("introduced_version", introducedVersion)
                .append("introduced_at", introducedAt);
        try {
            checks.insertOne(d);
            return id;
        } catch (MongoWriteException e) {
            Integer existing = findExistingId(stableKey);
            if (existing != null) return existing;
            throw e;
        }
    }

    @Override
    public void upsert(CheckCatalogRow row) {
        validateNoConflict(row);
        try {
            checks.replaceOne(
                    Filters.eq("check_id", row.checkId()),
                    toDocument(row),
                    new ReplaceOptions().upsert(true));
        } catch (MongoWriteException e) {
            validateNoConflict(row);
            if (e.getError().getCategory() != ErrorCategory.DUPLICATE_KEY) throw e;
            UpdateResult updated = checks.replaceOne(Filters.eq("check_id", row.checkId()), toDocument(row));
            if (updated.getMatchedCount() == 0) throw e;
        }
        meta.updateOne(Filters.eq("_id", 0),
                Updates.max("check_id_seq", row.checkId()),
                new UpdateOptions().upsert(true));
    }

    @Override
    public void updateDisplayAndDescription(int checkId,
                                            @Nullable String display,
                                            @Nullable String description) {
        checks.updateOne(Filters.eq("check_id", checkId),
                Updates.combine(
                        Updates.set("display", display),
                        Updates.set("description", description)));
    }

    private int nextCheckId() {
        Document d = meta.findOneAndUpdate(
                Filters.eq("_id", 0),
                Updates.inc("check_id_seq", 1),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        Number n = d == null ? null : d.get("check_id_seq", Number.class);
        if (n == null) throw new IllegalStateException("Mongo check_id_seq was not returned");
        return n.intValue();
    }

    static void alignCounterWithExistingRows(MongoCollection<Document> meta,
                                             MongoCollection<Document> checks) {
        Document d = checks.find().sort(Sorts.descending("check_id")).limit(1).first();
        if (d == null) return;
        Number n = d.get("check_id", Number.class);
        if (n == null || n.intValue() <= 0) return;
        meta.updateOne(Filters.eq("_id", 0),
                Updates.max("check_id_seq", n.intValue()),
                new UpdateOptions().upsert(true));
    }

    private void validateNoConflict(CheckCatalogRow row) {
        Document byId = checks.find(Filters.eq("check_id", row.checkId())).first();
        if (byId != null) {
            String stableKey = byId.getString("stable_key");
            if (!row.stableKey().equals(stableKey)) {
                throw new IllegalStateException("check_id " + row.checkId()
                        + " already maps to stable key " + stableKey + ", cannot import " + row.stableKey());
            }
        }
        Document byStableKey = checks.find(Filters.or(
                Filters.eq("stable_key", row.stableKey()),
                Filters.eq("_id", row.stableKey()))).first();
        if (byStableKey != null) {
            Number id = byStableKey.get("check_id", Number.class);
            if (id == null || id.intValue() != row.checkId()) {
                throw new IllegalStateException("stable key " + row.stableKey()
                        + " already maps to check_id " + (id == null ? "null" : id.intValue())
                        + ", cannot import as " + row.checkId());
            }
        }
    }

    private Integer findExistingId(String stableKey) {
        Document existing = checks.find(Filters.or(
                Filters.eq("stable_key", stableKey),
                Filters.eq("_id", stableKey))).first();
        if (existing == null) return null;
        Number n = existing.get("check_id", Number.class);
        return n == null ? null : n.intValue();
    }

    private static Document toDocument(CheckCatalogRow row) {
        return new Document("_id", row.stableKey())
                .append("check_id", row.checkId())
                .append("stable_key", row.stableKey())
                .append("display", row.display())
                .append("description", row.description())
                .append("introduced_version", row.introducedVersion())
                .append("introduced_at", row.introducedAt());
    }

    private static @Nullable CheckCatalogRow toRow(Document d) {
        Number id = d.get("check_id", Number.class);
        String stableKey = d.getString("stable_key");
        if (id == null || stableKey == null) return null;
        Number introducedAt = d.get("introduced_at", Number.class);
        return new CheckCatalogRow(
                id.intValue(),
                stableKey,
                d.getString("display"),
                d.getString("description"),
                d.getString("introduced_version"),
                introducedAt == null ? 0L : introducedAt.longValue());
    }
}
