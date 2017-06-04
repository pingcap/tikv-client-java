package com.pingcap.tikv.expression.aggregate;


import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.type.DecimalType;
import com.pingcap.tikv.type.FieldType;

public class Average extends TiUnaryFunctionExpression {

    public Average(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Avg;
    }

    @Override
    public FieldType getType() {
        return DecimalType.DEF_DECIMAL;
    }

    @Override
    public String getName() {
        return "avg";
    }
}