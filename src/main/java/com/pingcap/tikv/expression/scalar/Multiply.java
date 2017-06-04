package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.FieldType;

public class Multiply extends TiBinaryFunctionExpresson {
    public Multiply(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Mul;
    }

    @Override
    public String getName() {
        return "Multiply";
    }

    @Override
    public FieldType getType() {
        // TODO: Add type inference
        throw new UnsupportedOperationException();
    }
}
