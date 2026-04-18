package ac.grim.grimac.api.storage.history;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Platform-neutral rendered history output. Each platform's command layer converts this
 * into its own chat component model (Adventure Component, Bukkit ChatColor strings,
 * Fabric Text, …). The shared-impl HistoryService produces these; it knows nothing about
 * Bukkit or Fabric.
 */
@ApiStatus.Experimental
public record RenderedHistoryLine(List<Segment> segments) {

    public RenderedHistoryLine {
        segments = segments == null ? List.of() : List.copyOf(segments);
    }

    public sealed interface Segment {

        record Literal(String text) implements Segment {}

        record Styled(String text, Style style) implements Segment {}

        record Hover(Segment visible, List<Segment> hover) implements Segment {}

        record CheckRef(int checkId, String displayName) implements Segment {}

        record Duration(long ms) implements Segment {}

        record Timestamp(long epochMs, RelativeFormat format) implements Segment {}

        record PlayerRef(UUID uuid, @Nullable String currentName) implements Segment {}

        record ClickCommand(Segment visible, String commandToRun) implements Segment {}
    }

    public enum RelativeFormat {
        ABSOLUTE,
        AGO_COMPACT,
        AGO_VERBOSE
    }

    public record Style(@Nullable Color color, Set<Decoration> decorations) {

        public Style {
            decorations = decorations == null ? Set.of() : Set.copyOf(decorations);
        }

        public static Style of(@Nullable Color color) {
            return new Style(color, Set.of());
        }

        public static Style of(@Nullable Color color, Decoration... decs) {
            return new Style(color, Set.of(decs));
        }
    }

    public enum Color {
        BLACK, DARK_BLUE, DARK_GREEN, DARK_AQUA, DARK_RED, DARK_PURPLE, GOLD, GRAY,
        DARK_GRAY, BLUE, GREEN, AQUA, RED, LIGHT_PURPLE, YELLOW, WHITE
    }

    public enum Decoration {
        BOLD, ITALIC, UNDERLINED, STRIKETHROUGH, OBFUSCATED
    }
}
