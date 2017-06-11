package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.FieldType;

public class Count extends TiUnaryFunctionExpression {

    public Count(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Count;
    }

    @Override
    public String getName() {
        return "count";
    }

    @Override
    public FieldType getType() {
        return this.args.get(0).getType();
    }
}
