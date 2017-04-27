package com.pingcap.tikv.exception;

import com.pingcap.tikv.grpc.Kvrpcpb;

public class KeyException extends RuntimeException {
    private final Kvrpcpb.KeyError keyErr;

    public KeyException(Kvrpcpb.KeyError keyErr) {
        this.keyErr = keyErr;
    }

    public Kvrpcpb.KeyError getKeyErr() {
        return keyErr;
    }
}
