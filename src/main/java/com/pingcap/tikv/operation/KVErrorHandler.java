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
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class KVErrorHandler<RespT> implements ErrorHandler<RespT, Pdpb.Error> {
    private Function<RespT, Errorpb.Error> getRegionError;
    private RegionManager regionManager;
    private Kvrpcpb.Context ctx;

    public KVErrorHandler(RegionManager regionManager, Kvrpcpb.Context ctx, Function<RespT, Errorpb.Error> getRegionError) {
       this.ctx = ctx;
       this.regionManager = regionManager;
       this.getRegionError = getRegionError;
    }

    public void handle(RespT resp) {
        // this is for test. In reality, resp is never null.
        if (resp == null) {
            this.regionManager.onRequestFail(ctx.getRegionId(), ctx.getPeer().getStoreId());
            return;
        };
        Errorpb.Error error = getRegionError.apply(resp);
        if (error != null) {
            if (error.hasNotLeader()) {
                // update Leader here
                // no need update here. just let retry take control of this.
                this.regionManager.updateLeader(ctx.getRegionId(), ctx.getPeer().getStoreId());
                // TODO add sleep here
                throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE));
            }
            if (error.hasStoreNotMatch()) {
                this.regionManager.invalidateStore(ctx.getPeer().getStoreId());
                throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE));
            }

             // no need retry
            if (error.hasStaleEpoch()) {
                // regionManager.onRegionStale(context.getRegionId(), error.getStaleEpoch().getNewRegionsList());
                // StaleEpoch is not need to retry
                this.regionManager.onRegionStale(ctx.getRegionId(), error.getStaleEpoch().getNewRegionsList());
                throw new StatusRuntimeException(Status.fromCode(Status.Code.CANCELLED));
            }

            if (error.hasServerIsBusy()) {
                // TODO add some sleep here.
                throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE));
            }

            // do not handle it in this level. this can be retryed.
            if (error.hasStaleCommand()) {
                throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE));
            }

            if (error.hasRaftEntryTooLarge()) {
                throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE));
            }
            // for other errors, we only drop cache here.
            this.regionManager.invalidateRegion(ctx.getRegionId());
        }
    }
}
