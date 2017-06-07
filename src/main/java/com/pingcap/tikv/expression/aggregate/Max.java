package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.FieldType;

public class Max extends TiUnaryFunctionExpression {

    public Max(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Max;
    }

    @Override
    public String getName() {
        return "max";
    }

    @Override
    public FieldType getType() {
        return args.get(0).getType();
    }
}
