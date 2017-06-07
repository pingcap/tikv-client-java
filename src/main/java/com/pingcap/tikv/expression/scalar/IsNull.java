package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.BooleanType;
import com.pingcap.tikv.types.FieldType;

import static com.google.common.base.Preconditions.checkArgument;

public class IsNull extends TiUnaryFunctionExpression {
    public IsNull(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.IsNull;
    }

    @Override
    public String getName() {
        return "IsNull";
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        super.validateArguments();
    }
}
