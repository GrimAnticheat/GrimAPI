package ac.grim.grimac.internal.storage.retention;

import ac.grim.grimac.api.storage.DataStore;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.config.RetentionRule;
import ac.grim.grimac.api.storage.query.Deletes;
import org.jetbrains.annotations.ApiStatus;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Deletes expired rows per {@link RetentionRule}. Scheduling is external —
 * the host invokes {@link #sweepOnce()} on whatever cadence its platform
 * provides (Bukkit scheduler, cron-style, test-only tick, etc.).
 */
@ApiStatus.Internal
public final class RetentionSweeper {

    private final DataStore store;
    private final Map<Category<?>, RetentionRule> rules;
    private final Logger logger;

    public RetentionSweeper(DataStore store, Map<Category<?>, RetentionRule> rules, Logger logger) {
        this.store = store;
        this.rules = Map.copyOf(rules);
        this.logger = logger;
    }

    /** Runs one sweep. Blocks until each category's delete completes. */
    public void sweepOnce() {
        for (Map.Entry<Category<?>, RetentionRule> entry : rules.entrySet()) {
            Category<?> cat = entry.getKey();
            RetentionRule rule = entry.getValue();
            if (!rule.enabled() || rule.maxAgeDays() <= 0) continue;
            if (cat != Categories.SESSION && cat != Categories.VIOLATION) {
                // PlayerIdentity + Setting retention aren't wired through this
                // sweeper yet — their rules are parsed but not applied. Operators
                // opting in get defaults-off retention; flipping it on is a no-op
                // until a follow-up lands identity/setting eviction.
                continue;
            }
            long maxAgeMs = rule.maxAgeMs();
            try {
                CompletionStage<Void> stage = store.delete(cat, Deletes.olderThan(maxAgeMs));
                stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING, "[grim-datastore] retention sweep interrupted");
                return;
            } catch (CompletionException | java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                logger.log(Level.WARNING, "[grim-datastore] retention sweep failed for " + cat.id(), e);
            }
        }
    }
}
