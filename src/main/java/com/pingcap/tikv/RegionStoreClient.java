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

package com.pingcap.tikv;


import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.exception.KeyException;
import com.pingcap.tikv.exception.RegionException;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;
import com.pingcap.tikv.grpc.Kvrpcpb.*;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.TikvGrpc;
import com.pingcap.tikv.grpc.TikvGrpc.TikvBlockingStub;
import com.pingcap.tikv.grpc.TikvGrpc.TikvStub;
import com.pingcap.tikv.util.FutureObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RegionStoreClient extends AbstractGrpcClient<TikvBlockingStub, TikvStub> {
    private final Context                   context;
    private final TikvBlockingStub          blockingStub;
    private final TikvStub                  asyncStub;
    private final ManagedChannel            channel;

    private final int ReqTypeSelect = 101;
    private final int ReqTypeIndex = 102;

    public ByteString get(ByteString key, long version) {
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();
        GetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_GET, request, r -> r.getRegionError());
        return getHelper(resp);
    }

    public Future<ByteString> getAsync(ByteString key, long version) {
        FutureObserver<ByteString, GetResponse> responseObserver =
                new FutureObserver<>(this::getHelper);
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();

        callAsyncWithRetry(TikvGrpc.METHOD_KV_GET, request, responseObserver, r -> r.getRegionError());
        return responseObserver.getFuture();
    }

    private ByteString getHelper(GetResponse resp) {
        if (resp.hasError()) {
            throw new KeyException(resp.getError());
        }
        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
        return resp.getValue();
    }

    public List<KvPair> batchGet(Iterable<ByteString> keys, long version) {
        BatchGetRequest request = BatchGetRequest.newBuilder()
                .setContext(context)
                .addAllKeys(keys)
                .setVersion(version)
                .build();
        BatchGetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_BATCH_GET, request, r -> r.getRegionError());
        return batchGetHelper(resp);
    }

    public Future<List<KvPair>> batchGetAsync(Iterable<ByteString> keys, long version) {
        FutureObserver<List<KvPair>, BatchGetResponse> responseObserver =
                new FutureObserver<>(this::batchGetHelper);

        BatchGetRequest request = BatchGetRequest.newBuilder()
                .setContext(context)
                .addAllKeys(keys)
                .setVersion(version)
                .build();

        callAsyncWithRetry(TikvGrpc.METHOD_KV_BATCH_GET, request, responseObserver, r -> r.getRegionError());
        return responseObserver.getFuture();
    }

    private List<KvPair> batchGetHelper(BatchGetResponse resp) {
        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
        return resp.getPairsList();
    }

    public List<KvPair> scan(ByteString startKey, long version) {
        return scan(startKey, version, false);
    }

    public Future<List<KvPair>> scanAsync(ByteString startKey, long version) {
        return scanAsync(startKey, version, false);
    }

    public List<KvPair> scan(ByteString startKey, long version, boolean keyOnly) {
        ScanRequest request = ScanRequest.newBuilder()
                .setContext(context)
                .setStartKey(startKey)
                .setVersion(version)
                .setKeyOnly(keyOnly)
                .setLimit(getConf().getScanBatchSize())
                .build();
        ScanResponse resp = callWithRetry(TikvGrpc.METHOD_KV_SCAN, request, r -> r.getRegionError());
        return scanHelper(resp);
    }

    public Future<List<KvPair>> scanAsync(ByteString startKey, long version, boolean keyOnly) {
        FutureObserver<List<KvPair>, ScanResponse> responseObserver =
                new FutureObserver<>(this::scanHelper);

        ScanRequest request = ScanRequest.newBuilder()
                .setContext(context)
                .setStartKey(startKey)
                .setVersion(version)
                .setKeyOnly(keyOnly)
                .build();

        callAsyncWithRetry(TikvGrpc.METHOD_KV_SCAN, request, responseObserver, r -> r.getRegionError());
        return responseObserver.getFuture();
    }

    private List<KvPair> scanHelper(ScanResponse resp) {
        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
        return resp.getPairsList();
    }

    public SelectResponse coprocess(SelectRequest req, List<KeyRange> ranges) {
        Coprocessor.Request reqToSend = Coprocessor.Request.newBuilder()
                .setContext(context)
                .setTp(req.hasIndexInfo() ? ReqTypeIndex : ReqTypeSelect)
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();
        Coprocessor.Response resp = callWithRetry(TikvGrpc.METHOD_COPROCESSOR, reqToSend, r -> r.getRegionError());
        return coprocessorHelper(resp);
    }

    public Future<SelectResponse> coprocessAsync(SelectRequest req, List<KeyRange> ranges) {
        FutureObserver<SelectResponse, Coprocessor.Response> responseObserver =
                new FutureObserver<>(this::coprocessorHelper);
        Coprocessor.Request reqToSend = Coprocessor.Request.newBuilder()
                .setContext(context)
                .setTp(req.hasIndexInfo() ? ReqTypeIndex : ReqTypeSelect)
                .setData(req.toByteString())
                .addAllRanges(ranges)
                .build();
        callAsyncWithRetry(TikvGrpc.METHOD_COPROCESSOR, reqToSend, responseObserver, r -> r.getRegionError());
        return responseObserver.getFuture();
    }

    private SelectResponse coprocessorHelper(Coprocessor.Response resp) {
        try {
            if (resp.hasRegionError()) {
                throw new RegionException(resp.getRegionError());
            }
            SelectResponse selectResp = SelectResponse.parseFrom(resp.getData());
            if (selectResp.hasError()) {
                throw new SelectException(selectResp.getError());
            }
            return selectResp;
        } catch (InvalidProtocolBufferException e) {
            throw new TiClientInternalException("Error parsing protobuf for coprocessor response.", e);
        }
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
    }

    public static RegionStoreClient create(Region region, Store store, TiSession session) {
        RegionStoreClient client = null;
        try {
            HostAndPort address = HostAndPort.fromString(store.getAddress());
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address.getHostText(), address.getPort())
                    .usePlaintext(true)
                    .build();
            TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
            TikvStub asyncStub = TikvGrpc.newStub(channel);
            client = new RegionStoreClient(region, session, channel, blockingStub, asyncStub);
        } catch (Exception e) {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignore) {
                }
            }
            throw e;
        }
        return client;
    }

    private RegionStoreClient(Region region, TiSession session,
                              ManagedChannel channel,
                              TikvBlockingStub blockingStub,
                              TikvStub asyncStub) {
        super(session);
        checkNotNull(region, "Region is empty");
        checkArgument(region.getPeersCount() > 0, "Peer is empty");
        this.channel = channel;
        this.blockingStub = blockingStub;
        this.asyncStub = asyncStub;
        this.context = Context.newBuilder()
                .setRegionId(region.getId())
                .setRegionEpoch(region.getRegionEpoch())
                .setPeer(region.getPeers(0))
                .build();
    }

    @Override
    protected TikvBlockingStub getBlockingStub() {
        return blockingStub.withDeadlineAfter(getConf().getTimeout(),
                getConf().getTimeoutUnit());
    }

    @Override
    protected TikvStub getAsyncStub() {
        return asyncStub.withDeadlineAfter(getConf().getTimeout(),
                getConf().getTimeoutUnit());
    }
}
