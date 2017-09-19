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

import com.pingcap.tikv.kvproto.Errorpb;
import com.pingcap.tikv.kvproto.Kvrpcpb;
import com.pingcap.tikv.region.RegionManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import org.apache.log4j.Logger;

public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {
  private static final Logger logger = Logger.getLogger(KVErrorHandler.class);
  private Function<RespT, Errorpb.Error> getRegionError;
  private RegionManager regionManager;
  private Kvrpcpb.Context ctx;

  public KVErrorHandler(
      RegionManager regionManager,
      Kvrpcpb.Context ctx,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.ctx = ctx;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
  }

  public void handle(RespT resp) {
    // if resp is null, then region maybe out of dated. we need handle this on RegionManager.
    if (resp == null) {
      this.regionManager.onRequestFail(ctx.getRegionId(), ctx.getPeer().getStoreId());
      return;
    }

    Errorpb.Error error = getRegionError.apply(resp);
    if (error != null) {
      if (error.hasNotLeader()) {
        // update Leader here
        logger.warn(String.format("Thread %s: NotLeader Error with region id %d",
                                  Thread.currentThread().getId(), error.getNotLeader().getRegionId()));
        logger.warn(String.format("Thread %s: origin call with region id %d and store id %d",
                                  Thread.currentThread().getId(),
                                  ctx.getRegionId(),
                                  ctx.getPeer().getStoreId()));
        this.regionManager.updateLeader(ctx.getRegionId(), error.getNotLeader().getLeader().getStoreId());
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }
      if (error.hasStoreNotMatch()) {
        this.regionManager.invalidateStore(ctx.getPeer().getStoreId());
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }

      // no need retry. NewRegions is returned in this response. we just need update RegionManage's region cache.
      if (error.hasStaleEpoch()) {
        regionManager.onRegionStale(ctx.getRegionId(), error.getStaleEpoch().getNewRegionsList());
        this.regionManager.onRegionStale(
            ctx.getRegionId(), error.getStaleEpoch().getNewRegionsList());
        throw new StatusRuntimeException(Status.fromCode(Status.Code.CANCELLED).withDescription(error.toString()));
      }

      if (error.hasServerIsBusy()) {
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }

      if (error.hasStaleCommand()) {
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }

      if (error.hasRaftEntryTooLarge()) {
        throw new StatusRuntimeException(Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }
      // for other errors, we only drop cache here and throw a retryable exception.
      this.regionManager.invalidateRegion(ctx.getRegionId());
    }
  }
}
