package ac.grim.grimac.api.plugin;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public interface GrimPluginDescription {
    String getVersion();

    String getDescription();

    public @NotNull Collection<String> getAuthors();
}
