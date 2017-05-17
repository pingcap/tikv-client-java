package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;


import java.util.List;
public class TiExpr {
    private ExprType exprType;
    private String value;
    private static List<TiExpr> childrens;

    public boolean isExprTypeSupported() {
        return true;
    }

    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        builder.setTp(exprType);
        for(int i = 0; i < childrens.size(); i++) {
            builder.setChildren(i, childrens.get(i).toProto());
        }
        builder.setVal(ByteString.copyFromUtf8(value));
        return builder.build();
    }
}
