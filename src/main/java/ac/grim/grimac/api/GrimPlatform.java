package ac.grim.grimac.api;

import ac.grim.grimac.api.events.GrimEvent;
import ac.grim.grimac.api.util.InitLater;

import java.nio.file.Path;
import java.util.logging.Logger;

public interface GrimPlatform<PLAYER> {
     InitLater<GrimPlatform<?>> INSTANCE_HOLDER = new InitLater<>();

     static <PLAYER> GrimPlatform<PLAYER> getInstance() {
          return (GrimPlatform<PLAYER>) INSTANCE_HOLDER.get();
     }

     Path getConfigDirectory();
     Logger getLogger();
     GrimAPI<PLAYER> getApi();

     void callEvent(GrimEvent event);
}
