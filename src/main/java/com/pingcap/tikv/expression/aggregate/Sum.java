package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.FieldType;


public class Sum extends TiUnaryFunctionExpression {

    public Sum(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Sum;
    }

    @Override
    public FieldType getType() {
        return DecimalType.DEF_TYPE;
    }

    @Override
    public String getName() {
        return "sum";
    }
}
