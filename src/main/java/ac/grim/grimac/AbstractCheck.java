package ac.grim.grimac;

public interface AbstractCheck {

    String getCheckName();

    String getAlternativeName();

    String getConfigName();

    double getViolations();

    double getDecay();

    double getSetbackVL();

    void setEnabled(boolean enabled);

    void reload();

    default void onEndOfTick(){}
}
