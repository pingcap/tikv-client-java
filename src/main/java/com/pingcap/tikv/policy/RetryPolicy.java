/*
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
 */

package com.pingcap.tikv.policy;

import com.google.common.collect.ImmutableSet;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.operation.ErrorHandler;
import com.pingcap.tikv.util.BackOff;
import io.grpc.Status;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class RetryPolicy<RespT> {
  private static final Logger logger = LogManager.getFormatterLogger(RetryPolicy.class);

  private BackOff backOff = BackOff.ZERO_BACKOFF;

  // handles PD and TiKV's error.
  private ErrorHandler<RespT> handler;

  private ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);

  RetryPolicy(ErrorHandler<RespT> handler) {
    this.handler = handler;
  }

  private boolean checkNotRecoverableException(Status status) {
    return unrecoverableStatus.contains(status.getCode());
  }

  private void handleFailure(Exception e, String methodName, long millis) {
    Status status = Status.fromThrowable(e);
    if (checkNotRecoverableException(status)) {
      logger.error("Failed to recover from last grpc error calling %s.", methodName);
      throw new GrpcException(e);
    }
    doWait(millis);
  }

  private void doWait(long millis) {
    try {
      Thread.sleep( backOff.nextBackOffMillis() );
    } catch (InterruptedException e) {
      throw new RuntimeException( e );
    }
  }

  public RespT callWithRetry(Callable<RespT> proc, String methodName) {
    for(;true ;) {
      try {
        RespT result = proc.call();
        // Unlike usual case, error is not thrown as exception. Instead, the error info is
        // hidden in ErrorPb and PdPb.Error. We have to extract the error info out fist and
        // handle it accordingly.
        if (handler != null) {
          handler.handle(result);
        }
        return result;
      } catch (Exception e) {
        long nextBackMills  = this.backOff.nextBackOffMillis();
        if(nextBackMills == BackOff.STOP) {
          break;
        }
        handleFailure(e, methodName, nextBackMills);
      }
    }
    throw new GrpcException("failed to call");
  }

  public interface Builder<T> {
    RetryPolicy<T> create(ErrorHandler<T> handler);
  }
}
