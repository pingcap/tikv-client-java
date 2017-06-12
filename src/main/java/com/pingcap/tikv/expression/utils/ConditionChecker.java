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

package com.pingcap.tikv.expression.utils;


import static com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.types.FieldType;

public class ConditionChecker {
    private final TiIndexInfo index;
    private final int columnOffset;
    private final String pkName;
    public boolean shouldReserve;
    public final int length;

    public ConditionChecker(String pkName) {
        this.index = null;
        this.columnOffset = -1;
        this.pkName = pkName == null ? "" : pkName;
        this.shouldReserve = false;
        this.length = FieldType.UNSPECIFIED_LEN;
    }

    public boolean check(TiExpr expr) {
        checkNotNull(expr, "Expression should not be null");

        if (expr instanceof TiFunctionExpression) {
            return check((TiFunctionExpression) expr);
        } else if (expr instanceof TiColumnRef) {
            return check((TiColumnRef)expr);
        } else if (expr instanceof TiConstant) {
            return true;
        }
        throw new TiClientInternalException("Invalid Type for condition checker: " + expr.getClass().getSimpleName());
    }

    public boolean check(TiFunctionExpression expr) {
        if (expr instanceof And || expr instanceof Or) {
            return check(expr.getArg(0)) && check(expr.getArg(1));
        } else if (expr instanceof Equal ||
                expr instanceof NotEqual ||
                expr instanceof GreaterEqual ||
                expr instanceof GreaterThan ||
                expr instanceof LessEqual ||
                expr instanceof LessThan) {
            if (check((TiColumnRef)expr.getArg(0)) && expr.getArg(1) instanceof TiConstant) {
                return !(expr instanceof NotEqual) || length != FieldType.UNSPECIFIED_LEN;
            }
            if (check((TiColumnRef)expr.getArg(1)) && expr.getArg(0) instanceof TiConstant) {
                return !(expr instanceof NotEqual) || length != FieldType.UNSPECIFIED_LEN;
            }
        } else if (expr instanceof IsNull || expr instanceof IsTrue /* TODO: Add IsFalse when Proto ready*/) {
            if (expr.getArg(0) instanceof TiColumnRef) {
                return check((TiColumnRef)expr.getArg(0));
            }
            return false;
        } else if (expr instanceof Not) {
            // TODO: support "not like" and "not in" convert to access conditions.
            if (expr instanceof TiFunctionExpression) {
                if (expr instanceof In || expr instanceof Like) {
                    return false;
                }
            } else {
                return false;
            }
            return check(expr.getArg(0));
        } else if (expr instanceof In) {
            if (!(expr.getArg(0) instanceof TiColumnRef)) {
                return false;
            }
            if (!check((TiColumnRef)expr.getArg(0))) return false;
            for (int i = 1; i < expr.getArgSize(); i++) {
                if (!(expr.getArg(i) instanceof TiConstant)) {
                    return false;
                }
            }
            return true;
        } else if (expr instanceof Like) {
            return check((Like)expr);
        }
        return false;
    }

    public boolean check(TiColumnRef expr) {
        if (!pkName.isEmpty()) {
            return expr.getBindingColumn().matchName(pkName);
        }
        if (index != null) {
            String indexColumnName = index.getIndexColumns().get(columnOffset).getName();
            return expr.getBindingColumn().matchName(indexColumnName);
        }
        return true;
    }

    public boolean check(Like expr) {
        // TODO: Implement like pushdown
        return false;
    }
}
