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

package com.pingcap.tikv;

import com.google.protobuf.GeneratedMessageV3;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.grpc.Errorpb;
import io.grpc.MethodDescriptor;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;

public class RegionErrorHandler implements BiFunction<RegionRequestSender.TiResponse, RegionRequestSender.CmdType, Exception>{

    @Override
    public Exception apply(RegionRequestSender.TiResponse tiResponse, RegionRequestSender.CmdType cmdType) {
        Errorpb.Error error = tiResponse.getRegionError();
        // TiKV did not throw exception if resp is failed. We need check weather a resp
        // has regionError or not.
        if (tiResponse.hasRegionError()) {
            // When this happened, there are two reason for it.
            // 1. cached data is expired and region's leader is actually changed. Get leader info from PD.
            // 2. Region's leader is not yet ready. Region itself has to wait and get leader info from PD.
            if (error.hasNotLeader()) {
                // update Leader here
                // no need update here. just let retry take control of this.
//                    regionManager.updateLeader(context.getRegionId(), error.getNotLeader().getLeader().getStoreId());
                throw new RegionException(error);
            }

            // Current request's store id does not match the expected.
            if (error.hasStoreNotMatch()) {
                throw new RegionException(error);
            }

            // Region's version is expired. Retry later.
            if (error.hasStaleEpoch()) {
//                    regionManager.onRegionStale(context.getRegionId(), error.getStaleEpoch().getNewRegionsList());
                throw new IllegalStateException("StaleEpoch is not need to retry");
            }

            // Region is not found in current store
            if (error.hasRegionNotFound()) {
                // throw RegionNotFound exception
                throw new RegionException(error);
            }

            // kv has too many requests need to be proceed.
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
//                regionManager.invalidateRegion(context.getRegionId());
        }
        return null;
    };

    public RegionRequestSender.TiResponse toTiResponse(Callable rpc) {
       return null;
    }
    public RegionRequestSender.CmdType toCmdType(Callable rpc) {
        return null;
    }
}
