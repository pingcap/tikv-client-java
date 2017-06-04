package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.FieldType;

public class Divide extends TiBinaryFunctionExpresson {
    public Divide(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Div;
    }

    @Override
    public String getName() {
        return "Divide";
    }

    @Override
    public FieldType getType() {
        // TODO: Add type inference
        throw new UnsupportedOperationException();
    }
}
