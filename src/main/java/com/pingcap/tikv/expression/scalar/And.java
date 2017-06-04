package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.BooleanType;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.type.StringType;

import static com.google.common.base.Preconditions.checkArgument;

public class And extends TiBinaryFunctionExpresson {
    public And(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.And;
    }

    @Override
    public String getName() {
        return "And";
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }

    @Override
    protected void validateArguments() throws RuntimeException {
        // Validate 2 arguments
        super.validateArguments();
        // Validate 2 arguments are strings
        checkArgument(args.get(0).getType() instanceof BooleanType);
        checkArgument(args.get(1).getType() instanceof BooleanType);
    }
}
