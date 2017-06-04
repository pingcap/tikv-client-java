package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.type.BooleanType;
import com.pingcap.tikv.type.FieldType;

public class In extends TiFunctionExpression {
    public In(TiExpr...arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.In;
    }

    @Override
    public String getName() {
        return "IN";
    }

    @Override
    protected void validateArguments() throws RuntimeException {
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }
}
