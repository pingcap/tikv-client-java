package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.FieldType;

public class BitOr extends TiBinaryFunctionExpresson {
    public BitOr(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.BitOr;
    }

    @Override
    public String getName() {
        return "BitOr";
    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
