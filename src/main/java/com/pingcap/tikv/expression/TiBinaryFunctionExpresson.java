package com.pingcap.tikv.expression;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TiBinaryFunctionExpresson extends TiFunctionExpression {
    protected TiBinaryFunctionExpresson(TiExpr lhs, TiExpr rhs) {
        super(lhs, rhs);
    }
    public abstract String getName();

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        checkNotNull(this.args, "Arguments of " + getName() + " cannot be null");
        checkArgument(this.args.size() == 2, getName() + " takes only 2 argument");
    }
}
