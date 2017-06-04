package com.pingcap.tikv.expression;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TiUnaryFunctionExpression extends TiFunctionExpression {
    protected TiUnaryFunctionExpression(TiExpr... args) {
        super(args);
    }

    public abstract String getName();

    @Override
    protected void validateArguments() throws RuntimeException {
        checkNotNull(args, "Arguments of " + getName() + " cannot be null");
        checkArgument(args.size() == 1, getName() + " takes only 1 argument");
    }
}
