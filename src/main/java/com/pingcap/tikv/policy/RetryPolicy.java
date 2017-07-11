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

import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.operation.ErrorHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public abstract class RetryPolicy {
    private static final Logger logger = LogManager.getFormatterLogger(RetryPolicy.class);

    // Basically a leader recheck method
    private ErrorHandler handler;

    RetryPolicy(ErrorHandler handler) {
        this.handler = handler;
    }

    public <T> T callWithRetry(Callable<T> proc, String methodName) {
        while (true) {
            try {
                T result = proc.call();
                // TODO: null check is only for temporary. In theory, every rpc call need
                // have some mechanism to retry call. The reason we allow this having two reason:
                // 1. Test's resp is null
                // 2. getTimestamp pass a null error handler for now, since impl of it is not correct yet.
                if(handler != null) {
                    handler.handle(result);
                }
                return result;
            } catch (RegionException e) {
                logger.error("Failed to do last grpc recovering now %s.", methodName);
            } catch (Exception e) {
                logger.error("Failed to recover from last grpc error calling %s.", methodName);
                throw new GrpcException(e);
            }
        }
    }

    public interface Builder {
        RetryPolicy create(ErrorHandler handler);
    }
}