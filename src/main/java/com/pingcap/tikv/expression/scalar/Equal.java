package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.BooleanType;
import com.pingcap.tikv.types.FieldType;
import com.pingcap.tikv.types.ShortType;

public class Equal extends TiBinaryFunctionExpresson {
    public Equal(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.EQ;
    }

    @Override
    public String getName() {
        return "=";
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }
}
