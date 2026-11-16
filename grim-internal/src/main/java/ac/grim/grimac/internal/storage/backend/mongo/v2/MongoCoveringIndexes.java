package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.internal.storage.backend.IndexCoverage;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The indexes a collection already has, read once per {@code ensureStore} with
 * {@code listIndexes}, so the Mongo adapters create a default index only when no
 * existing index covers it.
 */
@ApiStatus.Internal
final class MongoCoveringIndexes {

    private final @Nullable List<IndexCoverage.Existing> existing;
    private final @NotNull String collection;
    private final @NotNull Logger logger;

    private MongoCoveringIndexes(@Nullable List<IndexCoverage.Existing> existing, @NotNull String collection,
                                 @NotNull Logger logger) {
        this.existing = existing;
        this.collection = collection;
        this.logger = logger;
    }

    /** Reads the collection's indexes. If they cannot be read, every default index is created as before. */
    static @NotNull MongoCoveringIndexes read(@NotNull MongoCollection<Document> c, @NotNull Logger logger) {
        String name = c.getNamespace().getCollectionName();
        try {
            List<IndexCoverage.Existing> out = new ArrayList<>();
            for (Document index : c.listIndexes()) out.add(toExisting(index));
            return new MongoCoveringIndexes(out, name, logger);
        } catch (RuntimeException e) {
            logger.log(Level.FINE, e, () -> "could not list the indexes of " + name
                + "; creating the default indexes without checking for covering ones");
            return new MongoCoveringIndexes(null, name, logger);
        }
    }

    /**
     * A {@code listIndexes} entry as an {@link IndexCoverage.Existing}. Partial, sparse,
     * hidden and collated indexes, and any key that is not an ascending or descending
     * field (text, hashed, geo, wildcard), are not usable.
     */
    static @NotNull IndexCoverage.Existing toExisting(@NotNull Document index) {
        Document key = index.get("key", Document.class);
        boolean usable = index.get("partialFilterExpression") == null
            && !Boolean.TRUE.equals(index.get("sparse"))
            && !Boolean.TRUE.equals(index.get("hidden"))
            && index.get("collation") == null;
        List<IndexCoverage.Column> keys = new ArrayList<>();
        if (key != null) {
            for (Map.Entry<String, Object> part : key.entrySet()) {
                if (part.getValue() instanceof Number direction && direction.doubleValue() != 0) {
                    keys.add(new IndexCoverage.Column(part.getKey(), direction.doubleValue() < 0));
                } else {
                    keys.add(new IndexCoverage.Column(null, false));
                    usable = false;
                }
            }
        }
        return new IndexCoverage.Existing(String.valueOf(index.get("name")), Boolean.TRUE.equals(index.get("unique")),
            usable, keys);
    }

    /** True when an existing index covers {@code fields}; logs which one, so the skipped default is visible. */
    boolean covered(@NotNull String defaultIndex, @NotNull List<String> fields, boolean unique) {
        if (existing == null) return false;
        IndexCoverage.Existing covering = null;
        for (IndexCoverage.Existing index : existing) {
            if (!IndexCoverage.covers(index, fields, unique)) continue;
            // The default itself needs no log line.
            if (isDefault(index.name(), defaultIndex)) return true;
            if (covering == null) covering = index;
        }
        if (covering == null) return false;
        String coveringName = covering.name();
        logger.info(() -> "Not creating index " + defaultIndex + " on " + collection + " " + fields
            + ": existing index " + coveringName + " already covers it");
        return true;
    }

    private static boolean isDefault(@NotNull String name, @NotNull String defaultIndex) {
        return name.equals(defaultIndex);
    }
}
