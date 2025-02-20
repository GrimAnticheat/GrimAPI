package ac.grim.grimac.api;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface GrimPluginDescription {
    String getVersion();

    String getDescription();

    public @NotNull List<String> getAuthors();
}
