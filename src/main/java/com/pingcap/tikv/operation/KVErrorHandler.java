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
import com.pingcap.tikv.kvproto.Pdpb;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.region.RegionManager;

import java.util.function.Function;

public class KVErrorHandler<RespT> implements ErrorHandler<RespT, Pdpb.Error> {
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
        // no need update here. just let retry take control of this.
        this.regionManager.updateLeader(ctx.getRegionId(), ctx.getPeer().getStoreId());
        // TODO add sleep here
        throw new RegionException(error);
      }
      if (error.hasStoreNotMatch()) {
        this.regionManager.invalidateStore(ctx.getPeer().getStoreId());
        throw new RegionException(error);
      }

      // no need retry. NewRegions is returned in this response. we just need update RegionManage's region cache.
      if (error.hasStaleEpoch()) {
        this.regionManager.onRegionStale(
            ctx.getRegionId(), error.getStaleEpoch().getNewRegionsList());
        this.regionManager.onRegionStale(
            ctx.getRegionId(), error.getStaleEpoch().getNewRegionsList());
        throw new RegionException(error);
      }

      if (error.hasServerIsBusy()) {
        // TODO add some sleep here.
        throw new RegionException(error);
      }

      if (error.hasStaleCommand()) {
        throw new RegionException(error);
      }

      if (error.hasRaftEntryTooLarge()) {
        throw new RegionException(error);
      }
      // for other errors, we only drop cache here and throw a retryable exception.
      this.regionManager.invalidateRegion(ctx.getRegionId());
    }
  }
}
