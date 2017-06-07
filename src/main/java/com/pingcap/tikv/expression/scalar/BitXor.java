package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.FieldType;

public class BitXor extends TiBinaryFunctionExpresson {
    public BitXor(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.BitXor;
    }

    @Override
    public String getName() {
        return "BitXOr";
    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
