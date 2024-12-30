package ac.grim.grimac.api.checks;


import ac.grim.grimac.api.AbstractCheck;
import ac.grim.grimac.api.AbstractProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface AbstractCheckManager {

    <T extends AbstractProcessor> void registerProcessor(@NotNull Class<T> clazz, @NotNull T processor, @NotNull ListenerType listenerType);

    void unregisterProcessor(AbstractProcessor processor);

    Collection<? extends AbstractProcessor> getAllProcessors();

    Collection<? extends AbstractCheck> getChecks();

    <T extends AbstractProcessor> T getProcessor(@NotNull Class<T> check);

}
