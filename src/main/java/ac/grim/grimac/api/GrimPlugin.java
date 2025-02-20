package ac.grim.grimac.api;

import java.io.File;
import java.util.logging.Logger;

public interface GrimPlugin {

    GrimPluginDescription getDescription();

    Logger getLogger();

    File getDataFolder();
}
