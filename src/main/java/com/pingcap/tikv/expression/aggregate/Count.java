package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.types.DataType;

public class Count extends TiFunctionExpression {

    public Count(TiExpr...arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Count;
    }

    @Override
    public String getName() {
        return "count";
    }

    @Override
    public DataType getType() {
        return this.args.get(0).getType();
    }
}
