package com.pingcap.tikv.expression;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.List;
import java.util.Map;

public class TiAggregateFunction extends TiExpr {
    public enum AggFunc {
        Count, FirstRow,
        GroupConcat, Max,
        Min, Sum, Average
    }

    static final Map<AggFunc, ExprType> opAggFunc = ImmutableMap.<AggFunc, ExprType>builder()
            .put(AggFunc.Count, ExprType.Count)
            .put(AggFunc.FirstRow, ExprType.First)
            .put(AggFunc.GroupConcat, ExprType.GroupConcat)
            .put(AggFunc.Max, ExprType.Max)
            .put(AggFunc.Min, ExprType.Min)
            .put(AggFunc.Sum, ExprType.Sum)
            .put(AggFunc.Average, ExprType.Avg)
            .build();

    private AggFunc aggFunc;
    private List<TiExpr> args;

    public static TiAggregateFunction create(AggFunc func, TiExpr... args) {
        return new TiAggregateFunction(func, args);
    }

    private TiAggregateFunction(AggFunc func, TiExpr... args) {
        this.args = ImmutableList.copyOf(args);
        this.aggFunc = func;
    }

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        ExprType type = opAggFunc.get(aggFunc);
        if (type == null) {
            throw new UnsupportedOperationException("aggFunc " + aggFunc.name() + " is not supported");
        }
        builder.setTp(type);

        builder.addAllChildren(TiFluentIterable
                .from(args)
                .transform(arg -> arg.toProto())
        );
        return builder.build();
    }

    @Override
    public FieldType getType() {
        return null;
    }
}
