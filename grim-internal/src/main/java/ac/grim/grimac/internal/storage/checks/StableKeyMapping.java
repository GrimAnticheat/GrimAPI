package ac.grim.grimac.internal.storage.checks;

import org.jetbrains.annotations.ApiStatus;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Committed table bridging legacy v0 check display names to phase-1 stable keys.
 * <p>
 * When v0 data is migrated (see {@code LegacyMigrator}), every distinct
 * {@code check_name_string} in v0's {@code grim_history_check_names} is looked up here.
 * If present, the mapped stable_key is interned. If absent, a defensive
 * {@code "legacy:<name>"} stable_key is used so the row is still addressable instead of
 * crashing the migration.
 * <p>
 * This is the one committed place to update when a check is renamed between 2.0 and
 * later versions — editing the value here preserves historical violations' identity.
 */
@ApiStatus.Internal
public final class StableKeyMapping {

    private static final Map<String, String> MAPPINGS = buildMappings();

    private StableKeyMapping() {}

    public static Optional<String> stableKeyFor(String legacyDisplayName) {
        if (legacyDisplayName == null) return Optional.empty();
        return Optional.ofNullable(MAPPINGS.get(legacyDisplayName.toLowerCase(Locale.ROOT)));
    }

    public static String legacyFallback(String legacyDisplayName) {
        return "legacy:" + legacyDisplayName.toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> buildMappings() {
        // Keys are lowercased display names from v0's grim_history_check_names. Values
        // are stable keys Check classes will declare in Layer 3.
        //
        // The list is deliberately small — phase 1 ships the bridge, not an exhaustive
        // 2.0 check catalogue. Unknown names hit the "legacy:" fallback so migration
        // always completes; the operator can rename them later via the registry.
        return Map.ofEntries(
                Map.entry("reach", "combat.reach"),
                Map.entry("timer", "movement.timer"),
                Map.entry("simulation", "movement.simulation"),
                Map.entry("badpacketsa", "badpackets.a"),
                Map.entry("badpacketsb", "badpackets.b"),
                Map.entry("badpacketsc", "badpackets.c"),
                Map.entry("badpacketsd", "badpackets.d"),
                Map.entry("badpacketse", "badpackets.e"),
                Map.entry("badpacketsf", "badpackets.f"),
                Map.entry("badpacketsg", "badpackets.g"),
                Map.entry("badpacketsx", "badpackets.x"),
                Map.entry("badpacketsn", "badpackets.n"),
                Map.entry("hitboxes", "combat.hitboxes"),
                Map.entry("post", "movement.post"),
                Map.entry("knockback", "movement.knockback"),
                Map.entry("autoclickera", "combat.autoclicker.a"),
                Map.entry("autoclickerb", "combat.autoclicker.b"),
                Map.entry("autoclickerc", "combat.autoclicker.c"),
                Map.entry("autoclickerd", "combat.autoclicker.d"),
                Map.entry("autoclickere", "combat.autoclicker.e"),
                Map.entry("crash", "movement.crash"),
                Map.entry("noslow", "movement.noslow"),
                Map.entry("exploit", "misc.exploit"));
    }
}
