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
import com.pingcap.tikv.region.RegionErrorReceiver;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import org.apache.log4j.Logger;

public class KVErrorHandler<RespT> implements ErrorHandler<RespT> {

  private static final Logger logger = Logger.getLogger(KVErrorHandler.class);
  private Function<RespT, Errorpb.Error> getRegionError;
  private RegionManager regionManager;
  private RegionErrorReceiver recv;
  private TiRegion ctxRegion;

  public KVErrorHandler(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      TiRegion ctxRegion,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.ctxRegion = ctxRegion;
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
  }

  private void onNotLeader(Errorpb.Error error) {
    // update Leader here
    logger.warn(String.format("Thread %s: NotLeader Error with region id %d",
        Thread.currentThread().getId(), error.getNotLeader().getRegionId()));
    logger.warn(String.format("Thread %s: origin call with region id %d and store id %d",
        Thread.currentThread().getId(),
        ctxRegion.getId(),
        ctxRegion.getLeader().getStoreId()));
    long newStoreId = error.getNotLeader().getLeader().getStoreId();
    regionManager.updateLeader(ctxRegion.getId(), newStoreId);

    recv.onNotLeader(this.regionManager.getRegionById(ctxRegion.getId()),
        this.regionManager.getStoreById(newStoreId));
    throw new StatusRuntimeException(
        Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
  }

  private void onStoreNotMatch(Errorpb.Error error) {
    logger.warn(String.format("Thread %s: Store Not Match happened with region id %d, store id %d",
        Thread.currentThread().getId(), ctxRegion.getId(),
        ctxRegion.getLeader().getStoreId()));

    regionManager.invalidateRegion(ctxRegion.getId());
    regionManager.invalidateStore(ctxRegion.getLeader().getStoreId());
    recv.onStoreNotMatch();
    throw new StatusRuntimeException(
        Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
  }

  private void onStaleEpoch(Errorpb.Error error) {
    this.regionManager.onRegionStale(
        ctxRegion.getId(), error.getStaleEpoch().getNewRegionsList());
    throw new StatusRuntimeException(
        Status.fromCode(Status.Code.CANCELLED).withDescription(error.toString()));
  }

  public void handle(RespT resp) {
    // if resp is null, then region maybe out of dated. we need handle this on RegionManager.
    if (resp == null) {
      regionManager.onRequestFail(ctxRegion.getId(), ctxRegion.getLeader().getStoreId());
      return;
    }

    Errorpb.Error error = getRegionError.apply(resp);
    if (error != null) {
      if (error.hasNotLeader()) {
        onNotLeader(error);
      }
      if (error.hasStoreNotMatch()) {
        onStoreNotMatch(error);
      }

      // no need retry. NewRegions is returned in this response. we just need update RegionManage's region cache.
      if (error.hasStaleEpoch()) {
        onStaleEpoch(error);
      }

      // when tikv server is busy, make current task goes to sleep and retry later. Throw an exception is enough since
      // such exception is caught outside and #RetryPolicy takes care retry and backoff.
      if (error.hasServerIsBusy()) {
        logger.warn("TiKV reports ServerIsBusy");
        throw new StatusRuntimeException(
            Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }

      if (error.hasStaleCommand()) {
        logger.warn("TiKV reports StalesCommand");
        throw new StatusRuntimeException(
            Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }

      if (error.hasRaftEntryTooLarge()) {
        logger.warn("TiKV reports RaftEntryTooLarge");
        throw new StatusRuntimeException(
            Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString()));
      }
      // for other errors, we only drop cache here and throw a retryable exception.
      logger.warn("TiKV reports Region error: " + error.getMessage());
      regionManager.invalidateRegion(ctxRegion.getId());
    }
  }
}
