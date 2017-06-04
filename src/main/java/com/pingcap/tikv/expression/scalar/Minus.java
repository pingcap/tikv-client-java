package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.FieldType;

public class Minus extends TiBinaryFunctionExpresson {
    public Minus(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Minus;
    }

    @Override
    public String getName() {
        return "Minus";
    }

    @Override
    public FieldType getType() {
        // TODO: Add type inference
        throw new UnsupportedOperationException();
    }
}
