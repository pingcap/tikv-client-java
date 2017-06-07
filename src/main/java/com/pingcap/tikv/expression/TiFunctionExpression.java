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

package com.pingcap.tikv.expression;


import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.List;

public abstract class TiFunctionExpression implements TiExpr {

    protected final List<TiExpr> args;

    protected TiFunctionExpression(TiExpr... args) {
        validateArguments(args);
        this.args = ImmutableList.copyOf(args);
    }

    protected abstract ExprType getExprType();

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();

        builder.setTp(getExprType());
        builder.addAllChildren(TiFluentIterable
                .from(args)
                .transform(arg -> arg.toProto())
        );

        return builder.build();
    }

    public abstract String getName();

    protected abstract void validateArguments(TiExpr... args) throws RuntimeException;
}
