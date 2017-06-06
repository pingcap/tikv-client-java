package com.pingcap.tikv.expression.aggregate;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.type.DecimalType;
import com.pingcap.tikv.type.FieldType;

public class First extends TiUnaryFunctionExpression {

        public First(TiExpr arg) {
            super(arg);
        }

        @Override
        protected ExprType getExprType() {
            return ExprType.Sum;
        }

        @Override
        public FieldType getType() {
            return DecimalType.DEF_DECIMAL;
        }

        @Override
        public String getName() {
            return "first";
        }
}
