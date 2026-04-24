package ac.grim.grimac.api.storage.backend;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record ApiVersion(int major, int minor) implements Comparable<ApiVersion> {

    public static final ApiVersion V1_0 = new ApiVersion(1, 0);

    public static final ApiVersion CURRENT = V1_0;

    public boolean isCompatibleWith(ApiVersion required) {
        if (this.major != required.major) return false;
        return this.minor >= required.minor;
    }

    @Override
    public int compareTo(ApiVersion other) {
        int m = Integer.compare(this.major, other.major);
        return m != 0 ? m : Integer.compare(this.minor, other.minor);
    }

    @Override
    public String toString() {
        return "v" + major + "." + minor;
    }
}
