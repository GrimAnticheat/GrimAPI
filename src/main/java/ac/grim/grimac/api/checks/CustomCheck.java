package ac.grim.grimac.api.checks;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.config.ConfigManager;
import lombok.Getter;
import lombok.Setter;

@Getter
public class CustomCheck implements AbstractCheck {

    protected final GrimUser player;
    protected final String checkName;
    private final String listenerGroup;

    public CustomCheck(GrimUser player, String checkName, ListenerGroup listenerGroup) {
        this.player = player;
        this.checkName = checkName;
        this.listenerGroup = listenerGroup.getName();
    }

    @Setter protected boolean enabled = true;
    protected boolean supported = true;

    @Override
    final public boolean isSupported() {
        return supported;
    }

    private long lastViolation;
    @Getter private double violations;

    protected double decay;
    protected double setbackVL;
    protected boolean experimental;

    public final boolean flag(String verbose) {
        if (player.getCheckManager().onCustomCheckViolation(this, verbose)) {
            lastViolation = System.currentTimeMillis();
            violations++;
            return true;
        }
        return false;
    }

    public final boolean flag() {
        return flag("");
    }

    @Override
    public String getConfigName() {
        return checkName;
    }

    @Override
    public final void reload(ConfigManager object) {
        onReload(object);
    }

    protected void onReload(ConfigManager config) {

    }

}
