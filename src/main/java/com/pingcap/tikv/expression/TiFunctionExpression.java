package com.pingcap.tikv.expression;


import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.List;

public abstract class TiFunctionExpression implements TiExpr {

    protected final List<TiExpr> args;

    protected TiFunctionExpression(TiExpr... args) {
        validateArguments();
        this.args = ImmutableList.copyOf(args);
    }

    protected abstract ExprType getExprType();

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();

        builder.setTp(getExprType());
        builder.addAllChildren(TiFluentIterable
                .from(args)
                .transform(arg -> arg.toProto())
        );

        return builder.build();
    }

    public abstract String getName();

    protected abstract void validateArguments() throws RuntimeException;
}
