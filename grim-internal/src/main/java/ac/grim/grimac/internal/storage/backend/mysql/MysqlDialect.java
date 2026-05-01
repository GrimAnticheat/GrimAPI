package ac.grim.grimac.internal.storage.backend.mysql;

import ac.grim.grimac.api.storage.config.TableNames;
import org.jetbrains.annotations.ApiStatus;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Per-flavor SQL strings and DDL for the MySQL-family backend.
 * <p>
 * Two implementations:
 * <ul>
 *   <li>{@link MysqlEightDialect} — genuine MySQL 8.0+. Uses functional indexes
 *       ({@code INDEX … ((LOWER(col)))}) and the aliased-row upsert form
 *       ({@code INSERT … AS new ON DUPLICATE KEY UPDATE col = new.col}, MySQL
 *       8.0.19+).</li>
 *   <li>{@link MariaDbDialect} — MariaDB 10.6+. MariaDB doesn't implement either
 *       of those: the players table carries a {@code STORED} generated column
 *       for the lowercased name with a plain B-tree index on it, and upserts
 *       use the legacy {@code ON DUPLICATE KEY UPDATE col = VALUES(col)} form
 *       (deprecated in MySQL 8.0.20 but never deprecated in MariaDB).</li>
 * </ul>
 * The dialect is selected at {@link MysqlBackend#init} time by probing
 * {@code SELECT VERSION()} — same shape as the SQLite backend's
 * {@link ac.grim.grimac.internal.storage.backend.sqlite.writers.UpserterFactory}
 * version-driven dialect split.
 * <p>
 * Schema migrations ({@link MysqlSchema#ensureInitialized}) are flavor-agnostic
 * — only the v0 baseline DDL needs a flavor split, because the
 * {@code current_name_lower} index storage differs.
 */
@ApiStatus.Internal
public interface MysqlDialect {

    /**
     * Fresh-database baseline DDL. Creates checks, players, sessions,
     * violations, settings tables for a brand-new database. The meta
     * table is created independently in {@link MysqlSchema}.
     */
    void applyBaseline(Statement s, TableNames t) throws SQLException;

    String upsertSessions(TableNames t);

    String upsertIdentities(TableNames t);

    String upsertSettings(TableNames t);

    /** Single-row case-insensitive name lookup, latest {@code last_seen} first. */
    String selectIdentityByName(TableNames t);

    /** Case-insensitive prefix lookup for the player-name autocomplete query. */
    String selectIdentitiesByNamePrefix(TableNames t);
}
