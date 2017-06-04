package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.type.FieldType;

public class Coalesce extends TiFunctionExpression {
    public Coalesce(TiExpr...args) {
        super(args);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Coalesce;
    }

    @Override
    public String getName() {
        return "Coalesce";
    }

    @Override
    protected void validateArguments() throws RuntimeException {

    }

    @Override
    public FieldType getType() {
        throw new UnsupportedOperationException();
    }
}
