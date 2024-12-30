package ac.grim.grimac.api.checks.type;

import com.github.retrooper.packetevents.event.PacketSendEvent;

public interface PacketSendListener {

    void onPacketSend(final PacketSendEvent event);

}
