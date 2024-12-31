package ac.grim.grimac.api.checks.listeners;

import com.github.retrooper.packetevents.event.PacketReceiveEvent;

public interface PreViaPacketReceiveListener {

    void onPreViaPacketReceive(final PacketReceiveEvent event);

}
