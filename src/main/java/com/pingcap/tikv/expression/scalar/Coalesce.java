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
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.types.DataType;

public class Coalesce extends TiFunctionExpression {
    public Coalesce(TiExpr...args) {
        super(args);
    }

    @Override
    protected ExprType getExprType() {
        return ExprType.Coalesce;
    }

    @Override
    public String getName() {
        return "Coalesce";
    }

    @Override
    protected void validateArguments(TiExpr... args) throws RuntimeException {

    }

    @Override
    public DataType getType() {
        throw new UnsupportedOperationException();
    }
}