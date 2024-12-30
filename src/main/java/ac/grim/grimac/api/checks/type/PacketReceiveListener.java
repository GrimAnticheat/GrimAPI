package ac.grim.grimac.api.checks.type;

import com.github.retrooper.packetevents.event.PacketReceiveEvent;

public interface PacketReceiveListener {

    void onPacketReceive(final PacketReceiveEvent event);

}
