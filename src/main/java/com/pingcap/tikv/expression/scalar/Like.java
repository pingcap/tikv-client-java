/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression.scalar;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiBinaryFunctionExpresson;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.BytesType;

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
    public DataType getType() {
        return IntegerType.DEF_BOOLEAN_TYPE;
    }

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {
        // Validate 2 arguments
        super.validateArguments();
        // Validate 2 arguments are strings
        checkArgument(this.args.get(0).getType() instanceof BytesType);
        checkArgument(this.args.get(1).getType() instanceof BytesType);
    }
}
