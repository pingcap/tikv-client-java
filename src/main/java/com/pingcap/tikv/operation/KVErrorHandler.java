/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv.operation;

import com.pingcap.tikv.RegionManager;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.grpc.Errorpb;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Pdpb;

import java.util.function.Function;

public class KVErrorHandler<RespT> implements ErrorHandler<RespT, Pdpb.Error> {
    private Function<RespT, Errorpb.Error> getRegionError;
    private RegionManager regionManager;
    private long regionID;

    public KVErrorHandler(RegionManager regionManager, long regionID, Function<RespT, Errorpb.Error> getRegionError) {
       this.regionID = regionID;
       this.regionManager = regionManager;
       this.getRegionError = getRegionError;
    }

    public void handle(RespT resp) {
        // this is for test. In reality, resp is never null.
        if (resp == null) return;
        Errorpb.Error error = getRegionError.apply(resp);
        if (error != null) {
            if (error.hasNotLeader()) {
                // update Leader here
                // no need update here. just let retry take control of this.
//                regionManager.updateLeader(context.getRegionId(), error.getNotLeader().getLeader().getStoreId());
                throw new RegionException(error);
            }
            if (error.hasRegionNotFound()) {
                // throw RegionNotFound exception
                throw new RegionException(error);
            }
            // no need retry
            if (error.hasStaleEpoch()) {
//                 regionManager.onRegionStale(context.getRegionId(), error.getStaleEpoch().getNewRegionsList());
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
//            regionManager.invalidateRegion(context.getRegionId());
        }
    }
}
