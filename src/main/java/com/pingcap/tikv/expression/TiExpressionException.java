package com.pingcap.tikv.expression;


public class TiExpressionException extends RuntimeException {
    public TiExpressionException(String msg) {
        super(msg);
    }
}
