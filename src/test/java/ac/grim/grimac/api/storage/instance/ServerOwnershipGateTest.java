package ac.grim.grimac.api.storage.instance;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerOwnershipGateTest {

    @Test
    void disabledGateAlwaysAllowsWrites() {
        ServerOwnershipGate gate = ServerOwnershipGate.disabled();

        gate.close("ignored");

        assertFalse(gate.enforced());
        assertTrue(gate.allowWrites());
    }

    @Test
    void enforcedGateRequiresOpenOwnership() {
        ServerOwnershipGate gate = new ServerOwnershipGate(true);

        assertTrue(gate.enforced());
        assertFalse(gate.allowWrites());

        gate.open(UUID.randomUUID(), UUID.randomUUID(), 30_000L, 5_000L);
        assertTrue(gate.allowWrites());

        gate.close("shutdown");
        assertFalse(gate.allowWrites());
        assertEquals("shutdown", gate.closeReason());
    }

    @Test
    void extendWithWrongTokenClosesGate() {
        ServerOwnershipGate gate = new ServerOwnershipGate(true);
        UUID startupId = UUID.randomUUID();
        UUID fence = UUID.randomUUID();

        gate.open(startupId, fence, 30_000L, 5_000L);
        gate.extend(startupId, UUID.randomUUID(), 30_000L, 5_000L);

        assertFalse(gate.allowWrites());
        assertEquals("ownership-token-mismatch", gate.closeReason());
    }

    @Test
    void localDeadlineExpiryClosesGate() throws InterruptedException {
        ServerOwnershipGate gate = new ServerOwnershipGate(true);

        gate.open(UUID.randomUUID(), UUID.randomUUID(), 1L, 0L);
        Thread.sleep(25L);

        assertFalse(gate.allowWrites());
        assertEquals("local-lease-deadline-expired", gate.closeReason());
    }
}
