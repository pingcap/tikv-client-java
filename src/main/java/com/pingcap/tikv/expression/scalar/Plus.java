package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.BooleanType;
import com.pingcap.tikv.types.FieldType;

public class Plus extends TiBinaryFunctionExpresson {
    public Plus(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Plus;
    }

    @Override
    public String getName() {
        return "Plus";
    }

    @Override
    public FieldType getType() {
        // TODO: Add type inference
        throw new UnsupportedOperationException();
    }
}
