package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.type.BooleanType;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.type.StringType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Like extends TiBinaryFunctionExpresson {
    public Like(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Like;
    }

    @Override
    public String getName() {
        return "LIKE";
    }

    @Override
    public FieldType getType() {
        return BooleanType.DEF_BOOLEAN_TYPE;
    }

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        // Validate 2 arguments
        super.validateArguments();
        // Validate 2 arguments are strings
        checkArgument(this.args.get(0).getType() instanceof StringType);
        checkArgument(this.args.get(1).getType() instanceof StringType);
    }
}
