package ac.grim.grimac.api.storage.verbose;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

/**
 * Append-only target for rendered verbose fields.
 */
@ApiStatus.Experimental
public interface VerboseSink {

    @NotNull VerboseSink key(@NotNull CharSequence key);

    @NotNull VerboseSink num(double value);

    @NotNull VerboseSink num(float value);

    @NotNull VerboseSink num(int value);

    @NotNull VerboseSink num(long value);

    @NotNull VerboseSink bool(boolean value);

    @NotNull VerboseSink text(@NotNull CharSequence value);

    static @NotNull VerboseSink into(@NotNull StringBuilder out) {
        return new StringBuilderSink(out);
    }

    final class StringBuilderSink implements VerboseSink {
        private final StringBuilder out;
        private boolean first = true;

        private StringBuilderSink(@NotNull StringBuilder out) {
            this.out = out;
        }

        @Override
        public @NotNull VerboseSink key(@NotNull CharSequence key) {
            if (!first) out.append(", ");
            first = false;
            out.append(key).append('=');
            return this;
        }

        @Override public @NotNull VerboseSink num(double value) { out.append(value); return this; }
        @Override public @NotNull VerboseSink num(float value)  { out.append(value); return this; }
        @Override public @NotNull VerboseSink num(int value)    { out.append(value); return this; }
        @Override public @NotNull VerboseSink num(long value)   { out.append(value); return this; }
        @Override public @NotNull VerboseSink bool(boolean value) { out.append(value); return this; }
        @Override public @NotNull VerboseSink text(@NotNull CharSequence value) {
            out.append(value);
            return this;
        }
    }
}
