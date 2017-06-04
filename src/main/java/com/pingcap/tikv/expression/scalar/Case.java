package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.type.FieldType;

public class Case extends TiFunctionExpression {
    public Case(TiExpr...arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Case;
    }

    @Override
    public String getName() {
        return "Case";
    }

    @Override
    protected void validateArguments() throws RuntimeException {

    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
