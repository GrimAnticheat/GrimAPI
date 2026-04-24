package ac.grim.grimac.api.storage.query;

import ac.grim.grimac.api.storage.model.PlayerIdentity;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.model.SettingRecord;
import ac.grim.grimac.api.storage.model.SettingScope;
import ac.grim.grimac.api.storage.model.ViolationRecord;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@ApiStatus.Experimental
public final class Queries {

    private Queries() {}

    public static ListSessionsByPlayer listSessionsByPlayer(UUID player, int pageSize, @Nullable Cursor cursor) {
        return new ListSessionsByPlayer(player, pageSize, cursor);
    }

    public static GetSessionById getSessionById(UUID sessionId) {
        return new GetSessionById(sessionId);
    }

    public static ListViolationsInSession listViolationsInSession(UUID sessionId, int pageSize, @Nullable Cursor cursor) {
        return new ListViolationsInSession(sessionId, pageSize, cursor);
    }

    public static GetPlayerIdentity getPlayerIdentity(UUID uuid) {
        return new GetPlayerIdentity(uuid);
    }

    public static GetPlayerIdentityByName getPlayerIdentityByName(String name) {
        return new GetPlayerIdentityByName(name);
    }

    public static GetSetting getSetting(SettingScope scope, String scopeKey, String key) {
        return new GetSetting(scope, scopeKey, key);
    }

    public record ListSessionsByPlayer(UUID player, int pageSize, @Nullable Cursor cursor) implements Query<SessionRecord> {}

    public record GetSessionById(UUID sessionId) implements Query<SessionRecord> {}

    public record ListViolationsInSession(UUID sessionId, int pageSize, @Nullable Cursor cursor) implements Query<ViolationRecord> {}

    public record GetPlayerIdentity(UUID uuid) implements Query<PlayerIdentity> {}

    public record GetPlayerIdentityByName(String name) implements Query<PlayerIdentity> {}

    public record GetSetting(SettingScope scope, String scopeKey, String key) implements Query<SettingRecord> {}
}
