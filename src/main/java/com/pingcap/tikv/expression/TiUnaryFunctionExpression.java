package com.pingcap.tikv.expression;

import com.pingcap.tikv.types.FieldType;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TiUnaryFunctionExpression extends TiFunctionExpression {
    private FieldType fieldType;
    protected TiUnaryFunctionExpression(TiExpr... args) {
        super(args);
    }

    public abstract String getName();

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        checkNotNull(args, "Arguments of " + getName() + " cannot be null");
        checkArgument(args.length == 1, getName() + " takes only 1 argument");
    }
}
