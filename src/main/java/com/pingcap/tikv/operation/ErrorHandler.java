package com.pingcap.tikv.operation;


public interface ErrorHandler<RespT> {
    com.pingcap.tikv.grpc.Errorpb.Error getError(RespT resp);

    default void handle(RespT resp) {
        com.pingcap.tikv.grpc.Errorpb.Error error = getError(resp);
        return;
    }
}
