package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.FieldType;

public class RightShift extends TiBinaryFunctionExpresson {
    public RightShift(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.RighShift;
    }

    @Override
    public String getName() {
        return "RightShift";
    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
