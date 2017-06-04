package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.BooleanType;
import com.pingcap.tikv.type.FieldType;

import static com.google.common.base.Preconditions.checkArgument;

public class BitAnd extends TiBinaryFunctionExpresson {
    public BitAnd(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.BitAnd;
    }

    @Override
    public String getName() {
        return "BitAnd";
    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
