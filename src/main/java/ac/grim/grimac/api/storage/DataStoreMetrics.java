package ac.grim.grimac.api.storage;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Experimental
public interface DataStoreMetrics {

    long queuedCount();

    long submittedTotal();

    long droppedOnOverflowTotal();

    long droppedOnErrorTotal();

    long writeBatchLatencyMsEma();

    long readLatencyMsEma();
}
