package ac.grim.grimac.api.storage;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public record DeletionReport(
        int sessionsDeleted,
        int violationsDeleted,
        int settingsDeleted,
        int identitiesDeleted,
        int blobsDeleted) {

    public static final DeletionReport EMPTY = new DeletionReport(0, 0, 0, 0, 0);

    public DeletionReport plus(DeletionReport other) {
        return new DeletionReport(
                sessionsDeleted + other.sessionsDeleted,
                violationsDeleted + other.violationsDeleted,
                settingsDeleted + other.settingsDeleted,
                identitiesDeleted + other.identitiesDeleted,
                blobsDeleted + other.blobsDeleted);
    }

    public int total() {
        return sessionsDeleted + violationsDeleted + settingsDeleted + identitiesDeleted + blobsDeleted;
    }
}
