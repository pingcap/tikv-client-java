package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.types.FieldType;
import com.pingcap.tikv.types.VarCharType;

public class GroupConcat extends TiUnaryFunctionExpression {

    public GroupConcat(TiExpr arg) {
        super(arg);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.GroupConcat;
    }

    @Override
    public String getName() {
        return "group_concat";
    }

    @Override
    public FieldType getType() {
        return VarCharType.DEF_TYPE;
    }
}
