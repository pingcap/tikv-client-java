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

import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.grpc.Errorpb;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Pdpb;

import java.util.function.Function;

public interface ErrorHandler<RespT, ErrorT> {
    void handle(RespT resp);
}

// upper class can not pass lambda
// Functional interface can pass lambda.
// PDErrorHandler is concrete.
// respT -> Error
// respT is abstract.
// getRegionError is lambda.
// How can  I pass getRegionError into this concrete class?
// quick answer is no if you do not do any abstraction. But yes if you add an abstraction.
// Abstraction is powerful but with cost. So think about it is it necessary to do this abstraction.
// The solution is that