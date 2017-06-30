package com.pingcap.tikv.operation;


import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.grpc.Errorpb;

public interface KvrpcErrorHandler<RespT> {
    Errorpb.Error getError(RespT resp);

    default void handle(RespT resp) {
        com.pingcap.tikv.grpc.Errorpb.Error error = getError(resp);
            if (error != null) {
                if (error.hasNotLeader()) {
                    // update Leader here
                    // no need update here. just let retry take control of this.
                    // regionManager.updateLeader(context.getRegionId(), error.getNotLeader().getLeader().getStoreId());
                    throw new RegionException(error);
                }
                if (error.hasRegionNotFound()) {
                    // throw RegionNotFound exception
                    throw new RegionException(error);
                }
                // no need retry
                if (error.hasStaleEpoch()) {
                    // regionManager.onRegionStale(context.getRegionId(), error.getStaleEpoch().getNewRegionsList());
                    throw new IllegalStateException("StaleEpoch is not need to retry");
                }

                if (error.hasServerIsBusy()) {
                    throw new RegionException(error);
                }

                // do not handle it in this level. this can be retryed.
                if (error.hasStaleCommand()) {
                    throw new RegionException(error);
                }

                if (error.hasRaftEntryTooLarge()) {
                    throw new RegionException(error);
                }
                // for other errors, we only drop cache here.
                // regionManager.invalidateRegion(context.getRegionId());
            }
    }
}
