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
        return "legacy:" + legacyDisplayName.toLowerCase(Locale.ROOT);
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
        m.put("badpacketsa", "badpackets.duplicate_slot");
        m.put("badpacketsd", "badpackets.invalid_pitch");
        m.put("badpacketse", "badpackets.invalid_position");
        m.put("badpacketsf", "badpackets.duplicate_sprint");
        m.put("badpacketsg", "badpackets.duplicate_sneak");
        m.put("badpacketsi", "badpackets.spoofed_abilities");
        m.put("badpacketsk", "badpackets.invalid_spectate");
        m.put("badpacketsl", "badpackets.invalid_dig");
        m.put("badpacketsm", "badpackets.respawn_alive");
        m.put("badpacketsn", "badpackets.invalid_teleport");
        m.put("badpacketso", "badpackets.invalid_keepalive");
        m.put("badpacketsp", "badpackets.invalid_click");
        m.put("badpacketsq", "badpackets.invalid_horse_jump");
        m.put("badpacketsu", "badpackets.invalid_block_placement");
        m.put("badpacketsv", "badpackets.slow_move");
        m.put("badpacketsy", "badpackets.oob_slot");
        m.put("badpackets1", "badpackets.slow_move");
        m.put("badpackets2", "badpackets.duplicate_inputs");
        // GP-only badpackets with no V3 behavioural equivalent — keep addressable
        m.put("badpacketsb", "legacy:badpacketsb");
        m.put("badpacketsc", "legacy:badpacketsc");
        m.put("badpacketsh", "legacy:badpacketsh");
        m.put("badpacketsj", "legacy:badpacketsj");
        m.put("badpacketsr", "legacy:badpacketsr");
        // GP.S (window_not_accepted transaction) shares behaviour with V3.T
        // once that check is implemented — unify them on the semantic key
        // so GP.S history migrates to the same id V3.T will populate.
        m.put("badpacketss", "badpackets.window_not_accepted");
        m.put("badpacketst", "legacy:badpacketst");
        m.put("badpacketsw", "legacy:badpacketsw");
        m.put("badpacketsx", "legacy:badpacketsx");
        m.put("badpacketsz", "legacy:badpacketsz");

        // ---------- crash/ ----------
        m.put("crasha", "crash.large_position");
        m.put("crashb", "crash.creative_while_not_creative");
        m.put("crashc", "crash.nan_position");
        m.put("crashd", "crash.lectern");
        m.put("crashe", "crash.low_view_distance");
        // Crash family letter-shift: GP F..I match V3 G..J by behaviour.
        m.put("crashf", "crash.button_crash");
        m.put("crashg", "crash.negative_sequence");
        m.put("crashh", "crash.invalid_tab_complete");
        m.put("crashi", "crash.invalid_bundle_slot");
        m.put("crashj", "crash.invalid_bundle_slot"); // V3-display alias for the same behaviour

        // ---------- combat/ ----------
        m.put("hitboxes", "combat.hitboxes");
        m.put("reach", "combat.reach");
        m.put("bedrockhitboxes", "combat.bedrock_hitboxes");
        m.put("bedrockreach", "combat.bedrock_reach");
        m.put("fairreach", "combat.fair_reach");
        m.put("keepsprint", "combat.keep_sprint");
        m.put("wallraytrace", "combat.wall_raytrace");
        m.put("elytratargetdistance", "combat.elytra_target_distance");
        m.put("selfinteract", "badpackets.self_hit"); // GP combat/SelfInteract → V3 badpackets/BadPacketsC
        m.put("multiinteracta", "legacy:multiinteracta");
        m.put("multiinteractb", "legacy:multiinteractb");

        // ---------- aim/ ----------
        m.put("aimduplicatelook", "aim.duplicate_look");
        m.put("aimmodulo360", "aim.modulo_360");
        m.put("aima", "aim.a");
        m.put("aimb", "aim.b");
        m.put("aimc", "aim.small_movement_assist");
        m.put("aimd", "aim.tick_assist_d");
        m.put("aime", "aim.tick_assist_e");
        m.put("aimf", "aim.f");
        m.put("aimg", "aim.g");
        m.put("aimh", "aim.h");
        m.put("aimi", "aim.i");
        m.put("looka", "legacy:looka");
        m.put("aimfold", "legacy:aimfold");
        m.put("aimgold", "legacy:aimgold");
        m.put("aimhold", "legacy:aimhold");

        // ---------- breaking/ ----------
        m.put("airliquidbreak", "breaking.air_liquid_break");
        m.put("farbreak", "breaking.far_break");
        m.put("fastbreak", "breaking.fast_break");
        m.put("invalidbreak", "breaking.invalid_break");
        m.put("multibreak", "breaking.multi_break");
        m.put("noswingbreak", "breaking.no_swing_break");
        m.put("positionbreaka", "breaking.position_break_a");
        m.put("positionbreakb", "breaking.position_break_b");
        m.put("rotationbreak", "breaking.rotation_break");
        m.put("wrongbreak", "breaking.wrong_break");
        m.put("impossiblebreak", "breaking.impossible_break");
        m.put("invalidpositionbreak", "breaking.invalid_position_break");

        // ---------- scaffolding/ ----------
        m.put("airliquidplace", "scaffolding.air_liquid_place");
        m.put("duplicaterotplace", "scaffolding.duplicate_rot_place");
        m.put("fabricatedplace", "scaffolding.fabricated_place");
        m.put("farplace", "scaffolding.far_place");
        m.put("invalidplacea", "scaffolding.invalid_place_a");
        m.put("invalidplaceb", "scaffolding.invalid_place_b");
        m.put("invalidplacec", "scaffolding.invalid_place_c");
        m.put("multiplace", "scaffolding.multi_place");
        m.put("positionplace", "scaffolding.position_place");
        m.put("rotationplace", "scaffolding.rotation_place");
        m.put("scaffolda", "scaffolding.scaffold_a");
        m.put("scaffoldb", "scaffolding.scaffold_b");
        m.put("scaffoldc", "scaffolding.scaffold_c");

        // ---------- chat/ + exploit/ (V3 folded chat into exploit) ----------
        m.put("chata", "exploit.blank_tab_complete");
        m.put("chatb", "exploit.spigot_antispam_bypass");
        m.put("chatc", "legacy:chatc");
        m.put("chatd", "exploit.chat_while_hidden");
        m.put("exploita", "legacy:exploita");             // GP anvil-length — no V3 equivalent
        m.put("exploitb", "exploit.invalid_book_edit");   // GP.B → V3.E
        m.put("exploitc", "exploit.invalid_plugin_channels");
        m.put("exploitd", "exploit.invalid_command_block");
        m.put("exploite", "exploit.invalid_book_edit");   // V3.E matches GP.ExploitB
        m.put("exploitf", "exploit.large_yaw");
        m.put("exploitg", "exploit.spigot_antispam_bypass"); // V3.G matches GP.ChatB
        m.put("exploith", "exploit.chat_while_hidden");   // V3.H matches GP.ChatD

        // ---------- velocity/ ----------
        m.put("antiexplosion", "velocity.anti_explosion");
        m.put("antikb", "velocity.anti_knockback");
        m.put("antiknockback", "velocity.anti_knockback");

        // ---------- prediction/ ----------
        m.put("simulation", "prediction.simulation");
        m.put("phase", "prediction.phase");
        m.put("angle", "prediction.angle");
        m.put("boatoffset", "prediction.boat_offset");
        m.put("elytra", "prediction.elytra_offset");
        m.put("happyghastoffset", "prediction.happy_ghast_offset");
        m.put("invalidstep", "prediction.invalid_step");
        m.put("nautilusoffset", "prediction.nautilus_offset");
        m.put("nofalloffset", "prediction.nofall_offset");
        m.put("nosneakslow", "prediction.no_sneak_slow");
        m.put("omnisprint", "prediction.omni_sprint");
        m.put("speed", "prediction.speed");
        m.put("strafe", "prediction.strafe");
        m.put("verticaloffset", "prediction.vertical_offset");

        // ---------- movement/ + groundspoof/ + post/ + ping/ + baritone/ ----------
        m.put("noslow", "movement.noslow");
        m.put("groundspoof", "legacy:groundspoof");
        m.put("nofall", "groundspoof.no_fall");
        m.put("nofallb", "groundspoof.no_fall_b");
        m.put("post", "post.invalid_order");
        m.put("transactionorder", "ping.invalid_transaction_order");
        m.put("baritone", "baritone.baritone");
        m.put("pinga", "ping.spoof_a");
        m.put("pingb", "ping.spoof_b");
        m.put("pingc", "ping.spoof_c");
        m.put("blinka", "ping.blink_a");
        m.put("blinkb", "ping.blink_b");
        m.put("blinkc", "ping.blink_c");
        m.put("blinkd", "ping.blink_d");
        m.put("blinke", "ping.blink_e");

        // ---------- timer/ ----------
        m.put("negativetimer", "timer.negative");
        m.put("negativetimercheck", "timer.negative");
        m.put("ticktimer", "timer.tick");
        m.put("timer", "timer.timer");
        m.put("timerlimit", "legacy:timerlimit");
        m.put("vehicletimer", "timer.vehicle");
        m.put("timervehicle", "timer.vehicle");
        m.put("slowtimerrate", "timer.slow_rate");
        m.put("timerrate", "timer.rate");

        // ---------- misc/ ----------
        m.put("brandspoofa", "misc.brand_spoof_a");
        m.put("brandspoofb", "misc.brand_spoof_b");
        m.put("brandspoofc", "misc.brand_spoof_c");
        m.put("skinblinker", "misc.skin_blinker");
        m.put("clientbrand", "legacy:clientbrand");

        // ---------- elytra/ sprint/ vehicle/ multiactions/ packetorder/ ----------
        // GP-only families that V3 collapsed into prediction or dropped outright.
        for (char c = 'a'; c <= 'i'; c++) m.put("elytra" + c, "legacy:elytra" + c);
        for (char c = 'a'; c <= 'g'; c++) m.put("sprint" + c, "legacy:sprint" + c);
        for (char c = 'a'; c <= 'f'; c++) m.put("vehicle" + c, "legacy:vehicle" + c);
        for (char c = 'a'; c <= 'g'; c++) m.put("multiactions" + c, "legacy:multiactions" + c);
        for (char c = 'a'; c <= 'p'; c++) m.put("packetorder" + c, "legacy:packetorder" + c);

        // ---------- V3-only families (live-write fallback safety net) ----------
        m.put("auraa", "aura.a");
        m.put("aurab", "aura.b");
        m.put("aurac", "aura.c");
        m.put("autofisha", "auto.fish_a");
        m.put("autoclickerlimit", "autoclicker.limit");
        m.put("chatbot", "bots.chat_bot");
        m.put("lqbfl", "exploiter.lqb_fake_lag");
        m.put("handa", "hand.attack_order");
        m.put("handb", "hand.dig_without_swing");
        m.put("handc", "hand.place_without_swing");
        m.put("handd", "hand.use_without_swing");
        m.put("hande", "hand.e");
        m.put("handf", "hand.invalid_hitbox");
        m.put("handg", "hand.item_change_while_digging");
        m.put("handh", "hand.nuker");
        m.put("invalida", "invalid.a");
        m.put("invalidb", "invalid.fake_rotations");
        m.put("inventorya", "inventory.rotation_while_open");
        m.put("inventoryb", "inventory.auto_totem");
        m.put("inventoryc", "inventory.action_while_open");
        m.put("inventoryd", "inventory.d");
        m.put("inventorye", "inventory.chest_stealer");
        m.put("inventoryf", "inventory.input_while_open");
        m.put("inventoryg", "inventory.impossible_movement");
        m.put("spama", "spam.offhand");
        m.put("spamb", "spam.sneak");
        m.put("spamc", "spam.elytra");
        m.put("spamd", "spam.book_edit");
        m.put("spame", "spam.recipe");
        m.put("spamf", "spam.once_per_tick_abuse");

        // ---------- cloud/ (V3 tickcloud punishment family) ----------
        m.put("latencyabuse", "cloud.latency_abuse");
        m.put("latencyabuseban", "cloud.punish.latency_abuse_ban");
        m.put("latencyabusekick", "cloud.punish.latency_abuse_kick");
        m.put("suspiciousaim", "cloud.suspicious_aim");
        m.put("suspiciousaimban", "cloud.punish.suspicious_aim_ban");
        m.put("suspiciousaimkick", "cloud.punish.suspicious_aim_kick");
        m.put("suspiciouscombat", "cloud.suspicious_combat");
        m.put("suspiciouscombatban", "cloud.punish.suspicious_combat_ban");
        m.put("suspiciouscombatkick", "cloud.punish.suspicious_combat_kick");
        m.put("suspiciousother", "cloud.suspicious_other");
        m.put("suspiciousotherban", "cloud.punish.suspicious_other_ban");
        m.put("suspiciousotherkick", "cloud.punish.suspicious_other_kick");

        return Map.copyOf(m);
    }
}
