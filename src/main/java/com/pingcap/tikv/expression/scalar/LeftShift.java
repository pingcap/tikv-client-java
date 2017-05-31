package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.FieldType;

public class LeftShift extends TiBinaryFunctionExpresson {
    public LeftShift(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.LeftShift;
    }

    @Override
    public String getName() {
        return "LeftShift";
    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
