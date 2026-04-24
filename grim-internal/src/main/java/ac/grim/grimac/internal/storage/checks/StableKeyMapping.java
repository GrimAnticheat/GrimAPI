package ac.grim.grimac.internal.storage.checks;

import org.jetbrains.annotations.ApiStatus;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Committed table bridging legacy check display names to the stable keys the
 * v1+ schema uses.
 * <p>
 * Two consumers:
 * <ul>
 *   <li>v0 → v1 migration: {@code LegacyMigrator} resolves every distinct
 *       {@code check_name_string} in {@code grim_history_check_names} through
 *       this map so historical violations land on the new schema under the
 *       right stable_key.</li>
 *   <li>Live-write fallback: {@code LiveWriteHooks} consults this map only
 *       when a Check hasn't declared a {@code stableKey} on its
 *       {@code @CheckData} / {@code CheckInfo}. This is a rollout safety net
 *       — every check should eventually declare its stable_key up front so
 *       the live path skips this map entirely.</li>
 * </ul>
 * <p>
 * Unknown names hit the {@link #legacyFallback} path so migration always
 * completes; operators can rename the fallback keys later through the check
 * registry if they want.
 * <p>
 * Source of truth: {@code <workspace>/.docs/check-mapping/MAPPING.md}. When
 * a check is renamed, retired, or added, update MAPPING.md first then port
 * the change here.
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
        return "grim.legacy." + legacyDisplayName.toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> buildMappings() {
        // Keys are lowercased display names from the legacy
        // grim_history_check_names table + any Check.getCheckName() a live
        // writer might emit. Values are the stable keys new Check classes
        // declare via @CheckData.stableKey / CheckInfo.stableKey.
        //
        // Categories below mirror MAPPING.md's per-subpackage layout.
        Map<String, String> m = new HashMap<>(220);

        // ---------- badpackets/ ----------
        m.put("badpacketsa", "grim.badpackets.duplicate_slot");
        m.put("badpacketsd", "grim.badpackets.invalid_pitch");
        m.put("badpacketse", "grim.badpackets.invalid_position");
        m.put("badpacketsf", "grim.badpackets.duplicate_sprint");
        m.put("badpacketsg", "grim.badpackets.duplicate_sneak");
        m.put("badpacketsi", "grim.badpackets.spoofed_abilities");
        m.put("badpacketsk", "grim.badpackets.invalid_spectate");
        m.put("badpacketsl", "grim.badpackets.invalid_dig");
        m.put("badpacketsm", "grim.badpackets.respawn_alive");
        m.put("badpacketsn", "grim.badpackets.invalid_teleport");
        m.put("badpacketso", "grim.badpackets.invalid_keepalive");
        m.put("badpacketsp", "grim.badpackets.invalid_click");
        m.put("badpacketsq", "grim.badpackets.invalid_horse_jump");
        m.put("badpacketsu", "grim.badpackets.invalid_block_placement");
        m.put("badpacketsv", "grim.badpackets.slow_move");
        m.put("badpacketsy", "grim.badpackets.oob_slot");
        m.put("badpackets1", "grim.badpackets.slow_move");
        m.put("badpackets2", "grim.badpackets.duplicate_inputs");
        // GP-only badpackets with no V3 behavioural equivalent — keep addressable
        m.put("badpacketsb", "grim.legacy.badpacketsb");
        m.put("badpacketsc", "grim.legacy.badpacketsc");
        m.put("badpacketsh", "grim.legacy.badpacketsh");
        m.put("badpacketsj", "grim.legacy.badpacketsj");
        m.put("badpacketsr", "grim.legacy.badpacketsr");
        // GP.S (window_not_accepted transaction) shares behaviour with V3.T
        // once that check is implemented — unify them on the semantic key
        // so GP.S history migrates to the same id V3.T will populate.
        m.put("badpacketss", "grim.badpackets.window_not_accepted");
        m.put("badpacketst", "grim.legacy.badpacketst");
        m.put("badpacketsw", "grim.legacy.badpacketsw");
        m.put("badpacketsx", "grim.legacy.badpacketsx");
        m.put("badpacketsz", "grim.legacy.badpacketsz");

        // ---------- crash/ ----------
        m.put("crasha", "grim.crash.large_position");
        m.put("crashb", "grim.crash.creative_while_not_creative");
        m.put("crashc", "grim.crash.nan_position");
        m.put("crashd", "grim.crash.lectern");
        m.put("crashe", "grim.crash.low_view_distance");
        // Crash family letter-shift: GP F..I match V3 G..J by behaviour.
        m.put("crashf", "grim.crash.button_crash");
        m.put("crashg", "grim.crash.negative_sequence");
        m.put("crashh", "grim.crash.invalid_tab_complete");
        m.put("crashi", "grim.crash.invalid_bundle_slot");
        m.put("crashj", "grim.crash.invalid_bundle_slot"); // V3-display alias for the same behaviour

        // ---------- combat/ ----------
        m.put("hitboxes", "grim.combat.hitboxes");
        m.put("reach", "grim.combat.reach");
        m.put("bedrockhitboxes", "grim.combat.bedrock_hitboxes");
        m.put("bedrockreach", "grim.combat.bedrock_reach");
        m.put("fairreach", "grim.combat.fair_reach");
        m.put("keepsprint", "grim.combat.keep_sprint");
        m.put("wallraytrace", "grim.combat.wall_raytrace");
        m.put("elytratargetdistance", "grim.combat.elytra_target_distance");
        m.put("selfinteract", "grim.badpackets.self_hit"); // GP combat/SelfInteract → V3 badpackets/BadPacketsC
        m.put("multiinteracta", "grim.legacy.multiinteracta");
        m.put("multiinteractb", "grim.legacy.multiinteractb");

        // ---------- aim/ ----------
        m.put("aimduplicatelook", "grim.aim.duplicate_look");
        m.put("aimmodulo360", "grim.aim.modulo_360");
        m.put("aima", "grim.aim.a");
        m.put("aimb", "grim.aim.b");
        m.put("aimc", "grim.aim.small_movement_assist");
        m.put("aimd", "grim.aim.tick_assist_d");
        m.put("aime", "grim.aim.tick_assist_e");
        m.put("aimf", "grim.aim.f");
        m.put("aimg", "grim.aim.g");
        m.put("aimh", "grim.aim.h");
        m.put("aimi", "grim.aim.i");
        m.put("looka", "grim.legacy.looka");
        m.put("aimfold", "grim.legacy.aimfold");
        m.put("aimgold", "grim.legacy.aimgold");
        m.put("aimhold", "grim.legacy.aimhold");

        // ---------- breaking/ ----------
        m.put("airliquidbreak", "grim.breaking.air_liquid_break");
        m.put("farbreak", "grim.breaking.far_break");
        m.put("fastbreak", "grim.breaking.fast_break");
        m.put("invalidbreak", "grim.breaking.invalid_break");
        m.put("multibreak", "grim.breaking.multi_break");
        m.put("noswingbreak", "grim.breaking.no_swing_break");
        m.put("positionbreaka", "grim.breaking.position_break_a");
        m.put("positionbreakb", "grim.breaking.position_break_b");
        m.put("rotationbreak", "grim.breaking.rotation_break");
        m.put("wrongbreak", "grim.breaking.wrong_break");
        m.put("impossiblebreak", "grim.breaking.impossible_break");
        m.put("invalidpositionbreak", "grim.breaking.invalid_position_break");

        // ---------- scaffolding/ ----------
        m.put("airliquidplace", "grim.scaffolding.air_liquid_place");
        m.put("duplicaterotplace", "grim.scaffolding.duplicate_rot_place");
        m.put("fabricatedplace", "grim.scaffolding.fabricated_place");
        m.put("farplace", "grim.scaffolding.far_place");
        m.put("invalidplacea", "grim.scaffolding.invalid_place_a");
        m.put("invalidplaceb", "grim.scaffolding.invalid_place_b");
        m.put("invalidplacec", "grim.scaffolding.invalid_place_c");
        m.put("multiplace", "grim.scaffolding.multi_place");
        m.put("positionplace", "grim.scaffolding.position_place");
        m.put("rotationplace", "grim.scaffolding.rotation_place");
        m.put("scaffolda", "grim.scaffolding.scaffold_a");
        m.put("scaffoldb", "grim.scaffolding.scaffold_b");
        m.put("scaffoldc", "grim.scaffolding.scaffold_c");

        // ---------- chat/ + exploit/ (V3 folded chat into exploit) ----------
        m.put("chata", "grim.exploit.blank_tab_complete");
        m.put("chatb", "grim.exploit.spigot_antispam_bypass");
        m.put("chatc", "grim.legacy.chatc");
        m.put("chatd", "grim.exploit.chat_while_hidden");
        m.put("exploita", "grim.legacy.exploita");             // GP anvil-length — no V3 equivalent
        m.put("exploitb", "grim.exploit.invalid_book_edit");   // GP.B → V3.E
        m.put("exploitc", "grim.exploit.invalid_plugin_channels");
        m.put("exploitd", "grim.exploit.invalid_command_block");
        m.put("exploite", "grim.exploit.invalid_book_edit");   // V3.E matches GP.ExploitB
        m.put("exploitf", "grim.exploit.large_yaw");
        m.put("exploitg", "grim.exploit.spigot_antispam_bypass"); // V3.G matches GP.ChatB
        m.put("exploith", "grim.exploit.chat_while_hidden");   // V3.H matches GP.ChatD

        // ---------- velocity/ ----------
        m.put("antiexplosion", "grim.velocity.anti_explosion");
        m.put("antikb", "grim.velocity.anti_knockback");
        m.put("antiknockback", "grim.velocity.anti_knockback");

        // ---------- prediction/ ----------
        m.put("simulation", "grim.prediction.simulation");
        m.put("phase", "grim.prediction.phase");
        m.put("angle", "grim.prediction.angle");
        m.put("boatoffset", "grim.prediction.boat_offset");
        m.put("elytra", "grim.prediction.elytra_offset");
        m.put("happyghastoffset", "grim.prediction.happy_ghast_offset");
        m.put("invalidstep", "grim.prediction.invalid_step");
        m.put("nautilusoffset", "grim.prediction.nautilus_offset");
        m.put("nofalloffset", "grim.prediction.nofall_offset");
        m.put("nosneakslow", "grim.prediction.no_sneak_slow");
        m.put("omnisprint", "grim.prediction.omni_sprint");
        m.put("speed", "grim.prediction.speed");
        m.put("strafe", "grim.prediction.strafe");
        m.put("verticaloffset", "grim.prediction.vertical_offset");

        // ---------- movement/ + groundspoof/ + post/ + ping/ + baritone/ ----------
        m.put("noslow", "grim.movement.noslow");
        m.put("groundspoof", "grim.legacy.groundspoof");
        m.put("nofall", "grim.groundspoof.no_fall");
        m.put("nofallb", "grim.groundspoof.no_fall_b");
        m.put("post", "grim.post.invalid_order");
        m.put("transactionorder", "grim.ping.invalid_transaction_order");
        m.put("baritone", "grim.baritone.baritone");
        m.put("pinga", "grim.ping.spoof_a");
        m.put("pingb", "grim.ping.spoof_b");
        m.put("pingc", "grim.ping.spoof_c");
        m.put("blinka", "grim.ping.blink_a");
        m.put("blinkb", "grim.ping.blink_b");
        m.put("blinkc", "grim.ping.blink_c");
        m.put("blinkd", "grim.ping.blink_d");
        m.put("blinke", "grim.ping.blink_e");

        // ---------- timer/ ----------
        m.put("negativetimer", "grim.timer.negative");
        m.put("negativetimercheck", "grim.timer.negative");
        m.put("ticktimer", "grim.timer.tick");
        m.put("timer", "grim.timer.timer");
        m.put("timerlimit", "grim.legacy.timerlimit");
        m.put("vehicletimer", "grim.timer.vehicle");
        m.put("timervehicle", "grim.timer.vehicle");
        m.put("slowtimerrate", "grim.timer.slow_rate");
        m.put("timerrate", "grim.timer.rate");

        // ---------- misc/ ----------
        m.put("brandspoofa", "grim.misc.brand_spoof_a");
        m.put("brandspoofb", "grim.misc.brand_spoof_b");
        m.put("brandspoofc", "grim.misc.brand_spoof_c");
        m.put("skinblinker", "grim.misc.skin_blinker");
        m.put("clientbrand", "grim.legacy.clientbrand");

        // ---------- elytra/ sprint/ vehicle/ multiactions/ packetorder/ ----------
        // GP-only families that V3 collapsed into prediction or dropped outright.
        for (char c = 'a'; c <= 'i'; c++) m.put("elytra" + c, "grim.legacy.elytra" + c);
        for (char c = 'a'; c <= 'g'; c++) m.put("sprint" + c, "grim.legacy.sprint" + c);
        for (char c = 'a'; c <= 'f'; c++) m.put("vehicle" + c, "grim.legacy.vehicle" + c);
        for (char c = 'a'; c <= 'g'; c++) m.put("multiactions" + c, "grim.legacy.multiactions" + c);
        for (char c = 'a'; c <= 'p'; c++) m.put("packetorder" + c, "grim.legacy.packetorder" + c);

        // ---------- V3-only families (live-write fallback safety net) ----------
        m.put("auraa", "grim.aura.a");
        m.put("aurab", "grim.aura.b");
        m.put("aurac", "grim.aura.c");
        m.put("autofisha", "grim.auto.fish_a");
        m.put("autoclickerlimit", "grim.autoclicker.limit");
        m.put("chatbot", "grim.bots.chat_bot");
        m.put("lqbfl", "grim.exploiter.lqb_fake_lag");
        m.put("handa", "grim.hand.attack_order");
        m.put("handb", "grim.hand.dig_without_swing");
        m.put("handc", "grim.hand.place_without_swing");
        m.put("handd", "grim.hand.use_without_swing");
        m.put("hande", "grim.hand.e");
        m.put("handf", "grim.hand.invalid_hitbox");
        m.put("handg", "grim.hand.item_change_while_digging");
        m.put("handh", "grim.hand.nuker");
        m.put("invalida", "grim.invalid.a");
        m.put("invalidb", "grim.invalid.fake_rotations");
        m.put("inventorya", "grim.inventory.rotation_while_open");
        m.put("inventoryb", "grim.inventory.auto_totem");
        m.put("inventoryc", "grim.inventory.action_while_open");
        m.put("inventoryd", "grim.inventory.d");
        m.put("inventorye", "grim.inventory.chest_stealer");
        m.put("inventoryf", "grim.inventory.input_while_open");
        m.put("inventoryg", "grim.inventory.impossible_movement");
        m.put("spama", "grim.spam.offhand");
        m.put("spamb", "grim.spam.sneak");
        m.put("spamc", "grim.spam.elytra");
        m.put("spamd", "grim.spam.book_edit");
        m.put("spame", "grim.spam.recipe");
        m.put("spamf", "grim.spam.once_per_tick_abuse");

        // ---------- cloud/ (V3 tickcloud punishment family) ----------
        m.put("latencyabuse", "grim.cloud.latency_abuse");
        m.put("latencyabuseban", "grim.cloud.punish.latency_abuse_ban");
        m.put("latencyabusekick", "grim.cloud.punish.latency_abuse_kick");
        m.put("suspiciousaim", "grim.cloud.suspicious_aim");
        m.put("suspiciousaimban", "grim.cloud.punish.suspicious_aim_ban");
        m.put("suspiciousaimkick", "grim.cloud.punish.suspicious_aim_kick");
        m.put("suspiciouscombat", "grim.cloud.suspicious_combat");
        m.put("suspiciouscombatban", "grim.cloud.punish.suspicious_combat_ban");
        m.put("suspiciouscombatkick", "grim.cloud.punish.suspicious_combat_kick");
        m.put("suspiciousother", "grim.cloud.suspicious_other");
        m.put("suspiciousotherban", "grim.cloud.punish.suspicious_other_ban");
        m.put("suspiciousotherkick", "grim.cloud.punish.suspicious_other_kick");

        return Map.copyOf(m);
    }
}
