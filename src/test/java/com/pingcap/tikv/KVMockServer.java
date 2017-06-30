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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.grpc.*;
import com.pingcap.tikv.grpc.Errorpb.Error;
import com.pingcap.tikv.grpc.Kvrpcpb.Context;
import com.pingcap.tikv.util.TiFluentIterable;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class KVMockServer extends TikvGrpc.TikvImplBase {


    private int port;
    private Server server;
    private Metapb.Region region;
    private TreeMap<String, String> dataMap = new TreeMap<>();
    private Map<ByteString, Integer> errorMap = new HashMap<>();

    // for KV error
    static final int ABORT = 1;
    static final int RETRY = 2;
    // for raw client error
    static final int NOT_LEADER = 3;
    static final int REGION_NOT_FOUND = 4;
    static final int KEY_NOT_IN_REGION = 5      ;
    static final int STALE_EPOCH         = 6       ;
    static final int SERVER_IS_BUSY        = 7   ;
    static final int STALE_COMMAND           = 8 ;
    static final int STORE_NOT_MATCH       = 9 ;
    static final int RAFT_ENTRY_TOO_LARGE = 10;


    void put(String key, String value) {
        dataMap.put(key, value);
    }
    void putError(String key, int code) {
        errorMap.put(ByteString.copyFromUtf8(key), code);
    }

    void clearAllMap() {
        dataMap.clear();
        errorMap.clear();
    }

    private void verifyContext(Context context) throws Exception {
        if (context.getRegionId() != region.getId() ||
                !context.getRegionEpoch().equals(region.getRegionEpoch()) ||
                !context.getPeer().equals(region.getPeers(0))) {
            throw new Exception();
        }
    }

    @Override
    public void rawGet(com.pingcap.tikv.grpc.Kvrpcpb.RawGetRequest request,
                       io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.RawGetResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            ByteString key = request.getKey();

            Kvrpcpb.RawGetResponse.Builder builder = Kvrpcpb.RawGetResponse.newBuilder();
            Integer errorCode = errorMap.get(key);
            Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
            if (errorCode != null) {
               setErrorInfo(errorCode, errBuilder);
                builder.setRegionError(errBuilder.build());
                //builder.setError("");
            } else {
                ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
                builder.setValue(value);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    /**
     */
    public void rawPut(com.pingcap.tikv.grpc.Kvrpcpb.RawPutRequest request,
                       io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.RawPutResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            ByteString key = request.getKey();

            Kvrpcpb.RawPutResponse.Builder builder = Kvrpcpb.RawPutResponse.newBuilder();
            Integer errorCode = errorMap.get(key);
            Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
            if (errorCode != null) {
               setErrorInfo(errorCode, errBuilder);
                builder.setRegionError(errBuilder.build());
                //builder.setError("");
            } else {
                ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    private void setErrorInfo(int errorCode, Errorpb.Error.Builder errBuilder) {
        if (errorCode == NOT_LEADER) {
                    errBuilder.setNotLeader(Errorpb.NotLeader.getDefaultInstance());
                } else if (errorCode == REGION_NOT_FOUND) {
                    errBuilder.setRegionNotFound(Errorpb.RegionNotFound.getDefaultInstance());
                } else if (errorCode == KEY_NOT_IN_REGION) {
                    errBuilder.setKeyNotInRegion(Errorpb.KeyNotInRegion.getDefaultInstance());
                } else if (errorCode == STALE_EPOCH) {
                    errBuilder.setStaleEpoch(Errorpb.StaleEpoch.getDefaultInstance());
                } else if (errorCode == STALE_COMMAND) {
                    errBuilder.setStaleCommand(Errorpb.StaleCommand.getDefaultInstance());
                } else if (errorCode == SERVER_IS_BUSY) {
                    errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
                } else if (errorCode == STORE_NOT_MATCH) {
                    errBuilder.setStoreNotMatch(Errorpb.StoreNotMatch.getDefaultInstance());
                } else if (errorCode == RAFT_ENTRY_TOO_LARGE) {
                    errBuilder.setRaftEntryTooLarge(Errorpb.RaftEntryTooLarge.getDefaultInstance());
                }
    }

    /**
     */
    public void rawDelete(com.pingcap.tikv.grpc.Kvrpcpb.RawDeleteRequest request,
                          io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.RawDeleteResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            ByteString key = request.getKey();

            Kvrpcpb.RawDeleteResponse.Builder builder = Kvrpcpb.RawDeleteResponse.newBuilder();
            Integer errorCode = errorMap.get(key);
            Errorpb.Error.Builder errBuilder = Errorpb.Error.newBuilder();
            if (errorCode != null) {
               setErrorInfo(errorCode, errBuilder);
                builder.setRegionError(errBuilder.build());
                //builder.setError("");
            } else {
                ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }
    @Override
    public void kvGet(com.pingcap.tikv.grpc.Kvrpcpb.GetRequest request,
                      io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.GetResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            ByteString key = request.getKey();


            Kvrpcpb.GetResponse.Builder builder = Kvrpcpb.GetResponse.newBuilder();
            Integer errorCode = errorMap.get(key);
            Kvrpcpb.KeyError.Builder errBuilder = Kvrpcpb.KeyError.newBuilder();
            if (errorCode != null) {
                if (errorCode == ABORT) {
                    errBuilder.setAbort("ABORT");
                } else if (errorCode == RETRY) {
                    errBuilder.setRetryable("Retry");
                }
                builder.setError(errBuilder);
            } else {
                ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
                builder.setValue(value);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public void kvScan(com.pingcap.tikv.grpc.Kvrpcpb.ScanRequest request,
                       io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.ScanResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            ByteString key = request.getStartKey();

            Kvrpcpb.ScanResponse.Builder builder = Kvrpcpb.ScanResponse.newBuilder();
            Error.Builder errBuilder = Error.newBuilder();
            Integer errorCode = errorMap.get(key);
            if (errorCode != null) {
                if (errorCode == ABORT) {
                    errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
                }
                builder.setRegionError(errBuilder.build());
            } else {
                ByteString startKey = request.getStartKey();
                SortedMap<String, String> kvs = dataMap.tailMap(startKey.toStringUtf8());
                builder.addAllPairs(kvs.entrySet().stream().map(kv -> Kvrpcpb.KvPair.newBuilder()
                        .setKey(ByteString.copyFromUtf8(kv.getKey()))
                        .setValue(ByteString.copyFromUtf8(kv.getValue()))
                        .build()).collect(Collectors.toList()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public void kvBatchGet(com.pingcap.tikv.grpc.Kvrpcpb.BatchGetRequest request,
                           io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Kvrpcpb.BatchGetResponse> responseObserver) {
        try {
            verifyContext(request.getContext());
            if (request.getVersion() == 0) {
                throw new Exception();
            }
            List<ByteString> keys = request.getKeysList();

            Kvrpcpb.BatchGetResponse.Builder builder = Kvrpcpb.BatchGetResponse.newBuilder();
            Error.Builder errBuilder = Error.newBuilder();
            ImmutableList.Builder<Kvrpcpb.KvPair> resultList = ImmutableList.builder();
            for (ByteString key : keys) {
                Integer errorCode = errorMap.get(key);
                if (errorCode != null) {
                    if (errorCode == ABORT) {
                        errBuilder.setServerIsBusy(Errorpb.ServerIsBusy.getDefaultInstance());
                    }
                    builder.setRegionError(errBuilder.build());
                    break;
                } else {
                    ByteString value = ByteString.copyFromUtf8(dataMap.get(key.toStringUtf8()));
                    resultList.add(Kvrpcpb.KvPair.newBuilder().setKey(key)
                                                              .setValue(value)
                                                              .build());
                }
            }
            builder.addAllPairs(resultList.build());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    @Override
    public void coprocessor(com.pingcap.tikv.grpc.Coprocessor.Request requestWrap,
                            io.grpc.stub.StreamObserver<com.pingcap.tikv.grpc.Coprocessor.Response> responseObserver) {
        try {
            verifyContext(requestWrap.getContext());

            SelectRequest request = SelectRequest.parseFrom(requestWrap.getData());
            if (request.getStartTs() == 0) {
                throw new Exception();
            }

            List<Coprocessor.KeyRange> keyRanges = requestWrap.getRangesList();

            Coprocessor.Response.Builder builderWrap = Coprocessor.Response.newBuilder();
            SelectResponse.Builder builder = SelectResponse.newBuilder();
            com.pingcap.tidb.tipb.Error.Builder errBuilder = com.pingcap.tidb.tipb.Error.newBuilder();

            for (Coprocessor.KeyRange keyRange : keyRanges) {
                Integer errorCode = errorMap.get(keyRange.getStart());
                if (errorCode != null) {
                    if (errorCode == ABORT) {
                        errBuilder.setCode(errorCode);
                        errBuilder.setMsg("whatever");
                    }
                    builder.setError(errBuilder.build());
                    break;
                } else {
                    ByteString startKey = keyRange.getStart();
                    SortedMap<String, String> kvs = dataMap.tailMap(startKey.toStringUtf8());
                    builder.addAllChunks(TiFluentIterable.from(kvs.entrySet()).filter(Objects::nonNull)
                            .stopWhen(kv -> kv.getKey().compareTo(keyRange.getEnd().toStringUtf8()) > 0)
                            .transform(kv -> Chunk.newBuilder()
                                    .setRowsData(ByteString.copyFromUtf8(kv.getValue()))
                                    .build()));
                }
            }

            responseObserver.onNext(builderWrap.setData(builder.build()
                                                        .toByteString())
                                               .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asRuntimeException());
        }
    }

    int start(Metapb.Region region) throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start();

        this.region = region;
        Runtime.getRuntime().addShutdownHook(new Thread(KVMockServer.this::stop));
        return port;
    }

    void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
