package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.ByItem;

import static com.google.common.base.Preconditions.checkNotNull;

public class TiByItem {
    private TiExpr expr;
    private boolean desc;

    public static TiByItem create(TiExpr expr, boolean desc) {
        return new TiByItem(expr, desc);
    }

    private TiByItem(TiExpr expr, boolean desc) {
        checkNotNull(expr, "Expr cannot be null for ByItem");

        this.expr = expr;
        this.desc = desc;
    }

    public ByItem toProto() {
        ByItem.Builder builder = ByItem.newBuilder();
        return builder.setExpr(expr.toProto())
                .setDesc(desc)
                .build();
    }
}
