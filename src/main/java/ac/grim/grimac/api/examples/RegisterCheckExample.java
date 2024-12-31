package ac.grim.grimac.api.examples;

import ac.grim.grimac.api.GrimUser;
import ac.grim.grimac.api.checks.ExternalCheck;
import ac.grim.grimac.api.checks.ListenerGroup;
import ac.grim.grimac.api.checks.listeners.PacketReceiveListener;
import ac.grim.grimac.api.events.GrimUserJoinEvent;
import com.github.retrooper.packetevents.event.PacketReceiveEvent;
import com.github.retrooper.packetevents.protocol.packettype.PacketType;
import com.github.retrooper.packetevents.protocol.world.Location;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientPlayerFlying;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.plugin.java.JavaPlugin;

public class RegisterCheckExample implements Listener {

    // register the listener
    public RegisterCheckExample(JavaPlugin plugin) {
        plugin.getServer().getPluginManager().registerEvents(this, plugin);
    }

    // listen to when a grim user joins, this will run on the player's netty thread
    @EventHandler
    public void onUserJoin(GrimUserJoinEvent event) {
        GrimUser user = event.getUser();
        // register checks
        user.getCheckManager().registerProcessor(ExampleCheckA.class, new ExampleCheckA(user));
        // we need to reload the user to apply all the changes
        user.reload();
    }

    // example check implementation
    private static class ExampleCheckA extends ExternalCheck implements PacketReceiveListener {

        public ExampleCheckA(GrimUser user) {
            super(user, "ExampleCheckA", ListenerGroup.PACKET);
        }

        private Location location = null;

        @Override
        public void onPacketReceive(PacketReceiveEvent event) {
            if (event.getPacketType() == PacketType.Play.Client.PLAYER_POSITION_AND_ROTATION || event.getPacketType() == PacketType.Play.Client.PLAYER_POSITION) {
                WrapperPlayClientPlayerFlying wrapper = new WrapperPlayClientPlayerFlying(event);
                Location lastLocation = location;
                location = wrapper.getLocation();
                if (lastLocation == null) return;
                final double deltaY = location.getY() - lastLocation.getY();
                if (deltaY == 0) return;
                debug(() -> "dy=" + deltaY + " vl=" + getViolations() + " supported=" + isSupported() + " disabled=" + isDisabled());
                if (deltaY > 0.4) flag("dy=" + deltaY);
            }
        }
    }

}
