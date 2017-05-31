package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.ByItem;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;


public class TiByItem {
    private TiExpr expr;
    private boolean desc;
    public ByItem toProto() {
        ByItem.Builder builder = ByItem.newBuilder();
        return builder.build();
    }

    public ExprType getExprType() {
        return this.expr.getExprType();
    }

}
