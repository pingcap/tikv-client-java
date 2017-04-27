package com.pingcap.tikv.exception;


import com.pingcap.tikv.grpc.Errorpb.Error;

public class RegionException extends RuntimeException {
    private final Error regionErr;

    public RegionException(Error regionErr) {
        this.regionErr = regionErr;
    }

    public Error getRegionErr() {
        return regionErr;
    }
}
