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


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.exception.*;
import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;
import com.pingcap.tikv.grpc.Kvrpcpb.*;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.TikvGrpc;
import com.pingcap.tikv.grpc.TikvGrpc.TikvBlockingStub;
import com.pingcap.tikv.grpc.TikvGrpc.TikvStub;
import com.pingcap.tikv.operation.KVErrorHandler;
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
    private RegionManager regionManager;

    private final int ReqTypeSelect = 101;
    private final int ReqTypeIndex = 102;

    public ByteString get(ByteString key, long version) {
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();
        KVErrorHandler<GetResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), GetResponse::getRegionError);
        GetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_GET, request, handler);
        return getHelper(resp);
    }

    public void rawPut(ByteString key, ByteString value,Context context) {
        RawPutRequest rawPutRequest = RawPutRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setValue(value)
                .build();

        KVErrorHandler<RawPutResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), RawPutResponse::getRegionError);
        RawPutResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_PUT, rawPutRequest, handler);
        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
    }

    public ByteString rawGet(ByteString key, Context context){
        RawGetRequest rawGetRequest = RawGetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .build();
        KVErrorHandler<RawGetResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), RawGetResponse::getRegionError);
        RawGetResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_GET, rawGetRequest, handler);

        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
        return resp.getValue();
    }

    public void rawDelete(ByteString key,Context context){
        RawDeleteRequest rawDeleteRequest = RawDeleteRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .build();

        KVErrorHandler<RawDeleteResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), RawDeleteResponse::getRegionError);
        RawDeleteResponse resp = callWithRetry(TikvGrpc.METHOD_RAW_DELETE, rawDeleteRequest,  handler);
        if (resp.hasRegionError()) {
            throw new RegionException(resp.getRegionError());
        }
    }

    public Future<ByteString> getAsync(ByteString key, long version) {
        FutureObserver<ByteString, GetResponse> responseObserver =
                new FutureObserver<>(this::getHelper);
        GetRequest request = GetRequest.newBuilder()
                .setContext(context)
                .setKey(key)
                .setVersion(version)
                .build();

        KVErrorHandler<GetResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), GetResponse::getRegionError);
        callAsyncWithRetry(TikvGrpc.METHOD_KV_GET, request, responseObserver, handler);
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
        KVErrorHandler<BatchGetResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), BatchGetResponse::getRegionError);
        BatchGetResponse resp = callWithRetry(TikvGrpc.METHOD_KV_BATCH_GET, request, handler);
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

        KVErrorHandler<BatchGetResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), BatchGetResponse::getRegionError);
        callAsyncWithRetry(TikvGrpc.METHOD_KV_BATCH_GET, request, responseObserver, handler);
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
        KVErrorHandler<ScanResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), ScanResponse::getRegionError);
        ScanResponse resp = callWithRetry(TikvGrpc.METHOD_KV_SCAN, request, handler);
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

        KVErrorHandler<ScanResponse> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), ScanResponse::getRegionError);
        callAsyncWithRetry(TikvGrpc.METHOD_KV_SCAN, request, responseObserver, handler);
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
        KVErrorHandler<Coprocessor.Response> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), Coprocessor.Response::getRegionError);
        Coprocessor.Response resp = callWithRetry(TikvGrpc.METHOD_COPROCESSOR, reqToSend, handler);
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
        KVErrorHandler<Coprocessor.Response> handler =
                new KVErrorHandler<>(regionManager, context.getRegionId(), Coprocessor.Response::getRegionError);
        callAsyncWithRetry(TikvGrpc.METHOD_COPROCESSOR, reqToSend, responseObserver, handler);
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

    public static final int MAX_CACHE_CAPACITY = 64;
    private static final Cache<String, ManagedChannel> connPool = CacheBuilder
            .newBuilder()
            .maximumSize(MAX_CACHE_CAPACITY)
            .build();

    public static RegionStoreClient create(Region region, Store store, TiSession session, RegionManager regionManager) {
        RegionStoreClient client = null;
        String addressStr = store.getAddress();
        try {
            ManagedChannel channel;
            channel = connPool.getIfPresent(addressStr);
            if (channel == null || channel.isShutdown()) {
                HostAndPort address = HostAndPort.fromString(addressStr);
                channel = ManagedChannelBuilder
                        .forAddress(address.getHostText(), address.getPort())
                        .usePlaintext(true)
                        .build();
                connPool.put(addressStr, channel);
            }
            TikvBlockingStub blockingStub = TikvGrpc.newBlockingStub(channel);
            TikvStub asyncStub = TikvGrpc.newStub(channel);
            client = new RegionStoreClient(region, session, regionManager, channel, blockingStub, asyncStub);
        } catch (Exception e) {
            if (client != null) {
                try {
                    connPool.invalidate(addressStr);
                    client.close();
                } catch (Exception ignore) {}
            }
            throw e;
        }
        return client;
    }

    private RegionStoreClient(Region region, TiSession session,
                              RegionManager regionManager,
                              ManagedChannel channel,
                              TikvBlockingStub blockingStub,
                              TikvStub asyncStub) {
        super(session);
        checkNotNull(region, "Region is empty");
        checkArgument(region.getPeersCount() > 0, "Peer is empty");
        this.channel = channel;
        this.regionManager = regionManager;
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
