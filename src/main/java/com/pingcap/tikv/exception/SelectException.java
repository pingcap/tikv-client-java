package com.pingcap.tikv.exception;

import com.pingcap.tidb.tipb.Error;

public class SelectException extends RuntimeException {
    private final Error err;
    public SelectException(Error err) {
        this.err = err;
    }

    public Error getError() {
        return err;
    }
}
