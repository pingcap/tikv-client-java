package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.type.BooleanType;
import com.pingcap.tikv.type.FieldType;

import static com.google.common.base.Preconditions.checkArgument;

public class Not extends TiUnaryFunctionExpression {
    public Not(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Not;
    }

    @Override
    public String getName() {
        return "Not";
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        super.validateArguments();
        checkArgument(this.args.get(0).getType() instanceof BooleanType);
    }
}
