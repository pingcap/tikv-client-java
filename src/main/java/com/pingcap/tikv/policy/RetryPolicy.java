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
import io.grpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public abstract class RetryPolicy {
    private static final Logger logger = LogManager.getFormatterLogger(RetryPolicy.class);

    // Basically a leader recheck method
    private ErrorHandler handler;

    private ImmutableSet<Status.Code> unrecoverableStatus = ImmutableSet.of(
            Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
            Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
            Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
            Status.Code.UNAUTHENTICATED
    );

    public RetryPolicy(ErrorHandler handler) {
        this.handler = handler;
    }

    protected abstract boolean shouldRetry(Exception e);

    protected boolean checkNotLeaderException(Status status) {
        // TODO: need a way to check this, for now all unknown exception
        return true;
    }

    protected boolean checkNotRecoverableException(Status status) {
        return unrecoverableStatus.contains(status.getCode());
    }

    public <T> T callWithRetry(Callable<T> proc, String methodName) {
        while (true) {
            try {
                T result = proc.call();
                handler.handle(result);
                return result;
            } catch (Exception e) {
                Status status = Status.fromThrowable(e);
                if (checkNotRecoverableException(status) || !shouldRetry(e)) {
                    logger.error("Failed to recover from last grpc error calling %s.", methodName);
                    throw new GrpcException(e);
                }
                try {
                } catch (Exception e1) {
                    // Ignore exception further spreading
                    logger.error("Error during grpc leader update.", e1);
                }
            }
        }
    }

    public interface Builder {
        RetryPolicy create(ErrorHandler handler);
    }
}
