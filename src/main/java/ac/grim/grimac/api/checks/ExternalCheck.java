package ac.grim.grimac.api.checks;

import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.config.ConfigManager;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Getter
public class ExternalCheck implements AbstractCheck {

    final GrimUser player;
    final String checkName;
    private final String listenerGroup;

    public ExternalCheck(GrimUser player, String checkName, ListenerGroup listenerGroup) {
        this.player = player;
        this.checkName = checkName;
        this.listenerGroup = listenerGroup.getName();
    }

    boolean supported = false;
    @Setter boolean enabled = true;
    @Setter int listeners = 0;
    private int reloadCount = 0;

    private long lastViolation;
    private double violations;

    double decay;
    double setbackVL;
    boolean experimental;

    long resetAfter = TimeUnit.MINUTES.toMillis(10);

    @Override
    public String getConfigName() {
        return checkName;
    }

    @Override
    public boolean supportsPlayer(GrimUser user) {
        return true;
    }

    @Override
    public void checkViolations(long time) {
        if (violations > 0 && lastViolation > 0 && time - lastViolation > resetAfter) violations = 0;
    }

    @Override
    public final void reload() {
        reload(player.getConfigManager());
    }

    @Override
    public final void reload(ConfigManager object) {
        reloadCount++;
        supported = supportsPlayer(player);
    }

    // Override this method in your check class to handle reloads
    public void onReload(ConfigManager config) {

    }

    @Override
    public void debug(Supplier<String> details) {
        player.getDebugManager().handleDebug(this, details);
    }

    public boolean flag(String verbose) {
        if (player.getCheckManager().handleViolation(player, this, verbose)) {
            lastViolation = System.currentTimeMillis();
            violations++;
            return true;
        }
        return false;
    }

    public boolean flag() {
        return flag("");
    }

}
