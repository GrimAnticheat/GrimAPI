package ac.grim.grimac.internal.storage.backend;

import ac.grim.grimac.internal.storage.backend.IndexCoverage.Column;
import ac.grim.grimac.internal.storage.backend.IndexCoverage.Existing;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("IndexCoverage: an existing index covers a default when it leads with the default's columns")
class IndexCoverageTest {

    @Test
    void compositeIndexesCoverTheirLeadingColumn() {
        // The dashboard indexes from the report against grim_violations' defaults.
        Existing sessionTimeId = index("dash_session", false, "session_id", "occurred_at", "id");
        Existing timeId = index("dash_time", false, "occurred_at", "id");
        Existing checkTime = index("dash_check", false, "check_id", "occurred_at");
        Existing playerTime = index("dash_player", false, "player_uuid", "occurred_at");

        assertTrue(IndexCoverage.covers(sessionTimeId, List.of("session_id"), false));
        assertTrue(IndexCoverage.covers(timeId, List.of("occurred_at"), false));
        assertTrue(IndexCoverage.covers(checkTime, List.of("check_id"), false));
        assertTrue(IndexCoverage.covers(playerTime, List.of("player_uuid"), false));
        assertTrue(IndexCoverage.covers(sessionTimeId, List.of("session_id", "occurred_at"), false));
    }

    @Test
    void aColumnThatIsNotLeadingIsNotCovered() {
        Existing timeId = index("dash_time", false, "occurred_at", "id");
        assertFalse(IndexCoverage.covers(timeId, List.of("id"), false));
        assertFalse(IndexCoverage.covers(timeId, List.of("id", "occurred_at"), false));
        assertFalse(IndexCoverage.covers(index("short", false, "session_id"),
            List.of("session_id", "occurred_at"), false));
    }

    @Test
    void columnNamesCompareWithoutCase() {
        assertTrue(IndexCoverage.covers(index("upper", false, "SESSION_ID", "OCCURRED_AT"),
            List.of("session_id"), false));
    }

    @Test
    void unusableIndexesNeverCover() {
        Existing partial = new Existing("partial", false, false,
            List.of(new Column("session_id", false), new Column("occurred_at", false)));
        Existing expression = new Existing("expr", false, true, List.of(new Column(null, false)));
        assertFalse(IndexCoverage.covers(partial, List.of("session_id"), false));
        assertFalse(IndexCoverage.covers(expression, List.of("session_id"), false));
    }

    @Test
    void directionsMatterOnlyForSeveralColumns() {
        Existing descTime = new Existing("desc", false, true,
            List.of(new Column("player_uuid", false), new Column("started_at", true)));
        assertTrue(IndexCoverage.covers(descTime, List.of("player_uuid", "-started_at"), false));
        // Read backwards, (a ASC, b DESC) serves (a DESC, b ASC) but not (a ASC, b ASC).
        assertTrue(IndexCoverage.covers(descTime, List.of("-player_uuid", "started_at"), false));
        assertFalse(IndexCoverage.covers(descTime, List.of("player_uuid", "started_at"), false));
        assertTrue(IndexCoverage.covers(index("asc", false, "started_at"), List.of("-started_at"), false));
    }

    @Test
    void aUniqueDefaultNeedsAUniqueIndexOnExactlyItsColumns() {
        assertTrue(IndexCoverage.covers(index("u", true, "check_id"), List.of("check_id"), true));
        assertFalse(IndexCoverage.covers(index("n", false, "check_id"), List.of("check_id"), true));
        // Unique on (check_id, stable_key) does not make check_id unique.
        assertFalse(IndexCoverage.covers(index("wide", true, "check_id", "stable_key"), List.of("check_id"), true));
        // A unique index still covers a non-unique default.
        assertTrue(IndexCoverage.covers(index("wide", true, "check_id", "stable_key"), List.of("check_id"), false));
    }

    private static Existing index(String name, boolean unique, String... columns) {
        List<Column> keys = new ArrayList<>();
        for (String column : columns) keys.add(new Column(column, false));
        return new Existing(name, unique, true, keys);
    }
}
