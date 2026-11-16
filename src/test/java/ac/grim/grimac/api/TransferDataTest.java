package ac.grim.grimac.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransferDataTest {
    @Test
    void requiresPositionAndVelocity() {
        assertThrows(IllegalStateException.class, () -> TransferData.builder().build());
        assertThrows(IllegalStateException.class, () -> TransferData.builder().position(1, 2, 3).build());
        assertThrows(IllegalStateException.class, () -> TransferData.builder().velocity(1, 2, 3).build());
    }

    @Test
    void storesPrimitivePositionVelocityAndOptionalRotation() {
        TransferData withoutRotation = TransferData.builder()
                .position(1.25, -2.5, 3.75)
                .velocity(-0.1, 0.2, -0.3)
                .build();

        assertEquals(1.25, withoutRotation.positionX());
        assertEquals(-2.5, withoutRotation.positionY());
        assertEquals(3.75, withoutRotation.positionZ());
        assertEquals(-0.1, withoutRotation.velocityX());
        assertEquals(0.2, withoutRotation.velocityY());
        assertEquals(-0.3, withoutRotation.velocityZ());
        assertFalse(withoutRotation.hasRotation());

        TransferData withRotation = TransferData.builder()
                .position(1, 2, 3)
                .velocity(4, 5, 6)
                .rotation(90, -30)
                .build();

        assertTrue(withRotation.hasRotation());
        assertEquals(90, withRotation.yaw());
        assertEquals(-30, withRotation.pitch());
    }
}
