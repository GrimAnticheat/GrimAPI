package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.category.Category;
import ac.grim.grimac.api.storage.kind.Operation;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.ops.EventStreamOps;
import ac.grim.grimac.api.storage.kind.ops.KeyValueScopedOps;
import ac.grim.grimac.api.storage.query.Queries;
import ac.grim.grimac.api.storage.query.Query;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Translates legacy {@link Query} instances into the equivalent v2
 * {@link Operation} so {@code DataStoreImpl.query()} can transparently
 * dispatch through the v2 adapter when a v2 route exists. Returns
 * {@code null} when the translation is not possible (unknown query type,
 * or a query with no v2 equivalent) — the caller falls back to legacy.
 *
 * <p>The translation is per-query-type, finite, and stable: each legacy
 * {@link Queries} factory method produces a specific record type that we
 * pattern-match here. New query types that don't have a v2 translation
 * silently fall through to legacy — no crash, just legacy performance.
 *
 * <p>Return-type adaptation (v2 returns {@code Optional<R>} for
 * single-entity lookups but the legacy query() returns {@code Page<R>})
 * is handled by the caller ({@code DataStoreImpl.translateV2Result}).
 *
 * <p><strong>Known cursor/sort compatibility gaps (codex review):</strong>
 * <ul>
 *   <li><strong>ListSessionsByPlayer:</strong> legacy cursor format is
 *       {@code started:hex}; v2 uses base64 {@code Cursors}. An old
 *       cursor from a pre-cutover page load will throw at v2 decode.
 *       Legacy sorts {@code started_at DESC, _id DESC}; v2 tie-breaks
 *       {@code _id ASC}. Post-cutover all new cursors are v2-format;
 *       the mismatch is a one-shot cutover-window edge case.</li>
 *   <li><strong>ListViolationsInSession:</strong> same cursor format
 *       incompatibility.</li>
 *   <li><strong>ListPlayersByNamePrefix:</strong> legacy orders
 *       {@code last_seen DESC}; v2 orders {@code current_name_lower ASC}.
 *       Autocomplete ordering regresses. A future sort-parameter on
 *       {@link EntityOps.PrefixIndexOp} would fix this.</li>
 * </ul>
 * These are acceptable for the cutover milestone: cursor invalidation
 * is a one-shot event (old pagination is re-started from page 1 by the
 * UI), and the name-prefix ordering regression is low-impact (rarely
 * visible to operators). A dedicated "sort by last_seen" variant of
 * PrefixIndexOp is a follow-up improvement.
 */
@ApiStatus.Internal
final class QueryToOperationTranslator {

    private QueryToOperationTranslator() {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    static @Nullable Operation<?> translate(Category<?> cat, Query<?> query) {
        // --- SESSION (Entity<UUID, ...>) ---
        if (query instanceof Queries.GetSessionById g) {
            return new EntityOps.GetByIdOp(cat, g.sessionId());
        }
        if (query instanceof Queries.ListSessionsByPlayer l) {
            return new EntityOps.FindByIndexOp(cat,
                "by_player_started", l.player(), l.cursor(), l.pageSize());
        }

        // --- VIOLATION (EventStream) ---
        if (query instanceof Queries.ListViolationsInSession l) {
            return new EventStreamOps.PageOp(cat,
                "session_id", l.sessionId(), l.cursor(), l.pageSize());
        }

        // --- PLAYER_IDENTITY (Entity<UUID, ...>) ---
        if (query instanceof Queries.GetPlayerIdentity g) {
            return new EntityOps.GetByIdOp(cat, g.uuid());
        }
        // Name lookup uses the declared by_name index. Backends that see
        // caseInsensitivePrefix=true are responsible for routing equality /
        // prefix scans through their lowercase comparison path.
        if (query instanceof Queries.GetPlayerIdentityByName g) {
            return new EntityOps.FindByIndexOp(cat,
                "by_name", g.name(), null, 1);
        }
        if (query instanceof Queries.ListPlayersByNamePrefix p) {
            // lowerPrefix is already lowered by the caller per the
            // Queries.ListPlayersByNamePrefix contract.
            return new EntityOps.PrefixIndexOp(cat,
                "by_name", p.lowerPrefix(), null, p.limit());
        }

        // --- SETTING (KeyValueScoped<SettingScope, byte[]>) ---
        // GetSetting returns Page<SettingRecord> in the legacy contract;
        // v2 KvOps.GetOp returns Optional<byte[]>. DataStoreImpl wraps
        // the byte[] back into a SettingRecord using the query's
        // scope/scopeKey/key to preserve the legacy result shape.
        if (query instanceof Queries.GetSetting g) {
            return new KeyValueScopedOps.GetOp<>(cat, g.scope(), g.scopeKey(), g.key());
        }

        // Unknown query type — no v2 translation. Caller falls back
        // to legacy Backend.read().
        return null;
    }
}
