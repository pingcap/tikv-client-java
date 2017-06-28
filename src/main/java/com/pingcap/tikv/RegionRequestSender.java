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

import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.grpc.Errorpb;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.policy.RetryPolicy;

public class RegionRequestSender {
    RegionManager regionCache;
    ReadOnlyPDClient client;
    String storeAddr;
    public RegionRequestSender(RegionManager regionCache, ReadOnlyPDClient client) {
        super();
        this.client = client;
        this.regionCache = regionCache;
    }

    public enum CmdType {
            CmdGet,
            CmdScan,
            CmdPrewrite,
            CmdCommit,
            CmdCleanup,
            CmdBatchGet,
            CmdBatchRollback,
            CmdScanLock,
            CmdResolveLock,
            CmdGC,
            CmdRawGet,
            CmdRawPut,
            CmdRawDelete,
            CmdCop,
    }
    public static class TiRequest {
        private CmdType type;
        private Kvrpcpb.GetRequest getRequest;
        private Kvrpcpb.ScanRequest scanRequest;
        Kvrpcpb.PrewriteRequest prewriteRequest;
        Kvrpcpb.CommitRequest commitRequest;
        Kvrpcpb.CleanupRequest cleanupRequest;
        private Kvrpcpb.BatchGetRequest batchGetRequest;
        private Kvrpcpb.BatchRollbackRequest batchRollbackRequest;
        Kvrpcpb.ScanLockRequest scanLockRequest;
        Kvrpcpb.ResolveLockRequest resolveLockRequest;
        Kvrpcpb.GCRequest gcRequest;
        Kvrpcpb.RawGetRequest rawGetRequest;
        Kvrpcpb.RawPutRequest rawPutRequest;
        Kvrpcpb.RawDeleteRequest rawDeleteRequest;
        Coprocessor.Request copRequest;
    }

    public class TiResponse {
        private CmdType type;
        Kvrpcpb.GCResponse gcResponse;
        Kvrpcpb.RawDeleteResponse rawDeleteResponse;
        Kvrpcpb.RawGetResponse rawGetResponse;
        Kvrpcpb.RawPutResponse rawPutResponse;
        Kvrpcpb.GetResponse getResponse;
        Kvrpcpb.BatchRollbackResponse batchRollbackResponse;
        Kvrpcpb.BatchGetResponse batchGetResponse;
        Kvrpcpb.CleanupResponse cleanupResponse;
        Kvrpcpb.PrewriteResponse prewriteResponse;
        Kvrpcpb.ResolveLockResponse resolveLockResponse;
        Kvrpcpb.ScanLockResponse scanLockResponse;
        Kvrpcpb.ScanResponse scanResponse;
        Kvrpcpb.CommitResponse commitResponse;
        Coprocessor.Response copResponse;

        private Errorpb.Error getRegionError() {
            switch (type) {
                case CmdGet:
                    return getResponse.getRegionError();
                case CmdBatchGet:
                    return batchGetResponse.getRegionError();
                case CmdBatchRollback:
                    return batchRollbackResponse.getRegionError();
                case CmdCleanup:
                    return cleanupResponse.getRegionError();
                case CmdCommit:
                    return commitResponse.getRegionError();
                case CmdCop:
                    return copResponse.getRegionError();
                case CmdGC:
                    return gcResponse.getRegionError();
                case CmdPrewrite:
                    return prewriteResponse.getRegionError();
                case CmdRawDelete:
                    return rawDeleteResponse.getRegionError();
                case CmdRawGet:
                    return rawGetResponse.getRegionError();
                case CmdRawPut:
                    return rawPutResponse.getRegionError();
                case CmdResolveLock:
                    return resolveLockResponse.getRegionError();
                case CmdScan:
                    return scanResponse.getRegionError();
                case CmdScanLock:
                    return scanLockResponse.getRegionError();
            }
            return null;
        }
    }
}
