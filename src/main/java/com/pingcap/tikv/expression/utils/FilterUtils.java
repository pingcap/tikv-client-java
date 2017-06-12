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


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.CastingException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.Pair;

import java.util.Comparator;
import java.util.List;

public class FilterUtils {
    public static void buildScan(List<TiExpr> conditions, TiTableInfo table) {
        
    }

    public static Pair<List<TiExpr>, List<TiExpr>> // Push down conditions, remain conditions
    detachTableScanConditions(List<TiExpr> conditions, TiTableInfo table) {
        String pkName = table.getPKName();
        if (pkName != null && pkName.isEmpty()) {
            return Pair.create(ImmutableList.of(), conditions);
        }
        ImmutableList.Builder<TiExpr> filterBuilder = ImmutableList.builder();
        ImmutableList.Builder<TiExpr> accessPointBuilder = ImmutableList.builder();
        for (TiExpr cond : conditions) {
            ConditionChecker checker = new ConditionChecker(pkName);
            // TODO: Pushdown with Not transformation
            if (checker.check(cond)) {
                filterBuilder.add(cond);
                continue;
            }
            accessPointBuilder.add(cond);
            if (checker.shouldReserve) {
                filterBuilder.add(cond);
            }
        }
        return Pair.create(accessPointBuilder.build(), filterBuilder.build());
    }

    public List<TiRange<Long>> buildTableRange(List<TiExpr> accessConditions) {
        if (accessConditions.size() == 0) {
            return ImmutableList.of(TiRange.FULL_LONG_RANGE);
        }

        List<TiRange<Long>> ranges = ImmutableList.of();
        for (TiExpr expr : accessConditions) {
            ranges = TiRange.intersect(ranges, exprToRange(expr));
        }
        ImmutableList.Builder<TiRange<Long>> keyRangesBuilder = ImmutableList.builder();
        for (TiRange<Long> range : ranges) {
            long lVal = range.getLowValue();
            if (range.isLeftOpen()) {
                lVal += 1;
            }
            long rVal = range.getHighValue();
            if (range.isRightOpen()) {
                rVal -= 1;
            }
            keyRangesBuilder.add(TiRange.create(lVal, rVal, false, false, Comparator.naturalOrder()));
        }
        return keyRangesBuilder.build();
    }

    public static List<TiRange<Long>> exprToRange(TiExpr expr) {
        // There should be no column ref expression sololy for now
        // TODO: Verify if it's needed
        if (expr instanceof TiFunctionExpression) {
            return exprToRange((TiFunctionExpression)expr);
        }
    }

    public static List<TiRange<Long>> exprToRange(TiFunctionExpression expr) {
        // TODO: Implement other expressions
        // refer to range.go
        if (expr instanceof GreaterEqual ||
                expr instanceof GreaterThan ||
                expr instanceof LessThan ||
                expr instanceof LessEqual ||
                expr instanceof Equal ||
                expr instanceof NotEqual) {
            return comparasionToRange(expr);
        } else if (expr instanceof And) {
            return TiRange.intersect(exprToRange(expr.getArg(0)),
                              exprToRange(expr.getArg(1)));
        } else if (expr instanceof Or) {
            return TiRange.union(exprToRange(expr.getArg(0)),
                          exprToRange(expr.getArg(1)));
        } else {
            // TODO: Check if back push needed for combination of CNF
            throw new TiClientInternalException("Expression cannot be cast ot KeyRange");
        }
    }

    public static List<TiRange<Long>> comparasionToRange(TiFunctionExpression expr) {
        if (expr.getArg(0) instanceof TiConstant &&
            expr.getArg(1) instanceof TiColumnRef) {
            // Reverse condition if lhs is constant
            if (expr instanceof GreaterThan) {
                expr = new LessThan(expr.getArg(1), expr.getArg(0));
            } else if (expr instanceof GreaterEqual) {
                expr = new LessEqual(expr.getArg(1), expr.getArg(0));
            } else if (expr instanceof LessThan) {
                expr = new GreaterThan(expr.getArg(1), expr.getArg(0));
            } else if (expr instanceof LessEqual) {
                expr = new LessThan(expr.getArg(1), expr.getArg(0));
            } else if (expr instanceof Equal) {
                expr = new Equal(expr.getArg(1), expr.getArg(0));
            } else if (expr instanceof NotEqual) {
                expr = new NotEqual(expr.getArg(1), expr.getArg(0));
            }
        }
        if (expr.getArg(0) instanceof TiColumnRef &&
            expr.getArg(1) instanceof TiConstant) {
            TiConstant constExpr = (TiConstant)expr.getArg(1);
            long constVal = 0;
            if (constExpr.isIntegerType()) {
                constVal = ((Number)constExpr.getValue()).longValue();
            } else {
                throw new TiClientInternalException("Non-integer-bind expression cannot be cast ot KeyRange");
            }
            if (expr instanceof GreaterThan) {
                return ImmutableList.of(TiRange.createLongRange(constVal, Long.MIN_VALUE, false, true));
            } else if (expr instanceof GreaterEqual) {
                return ImmutableList.of(TiRange.createLongRange(constVal, Long.MIN_VALUE, false, false));
            } else if (expr instanceof LessThan) {
                return ImmutableList.of(TiRange.createLongRange(Long.MIN_VALUE, constVal, false, true));
            } else if (expr instanceof LessEqual) {
                return ImmutableList.of(TiRange.createLongRange(Long.MIN_VALUE, constVal, false, false));
            } else if (expr instanceof Equal) {
                return ImmutableList.of(TiRange.createLongPoint(constVal));
            } else if (expr instanceof NotEqual) {
                return ImmutableList.of(
                        TiRange.createLongRange(Long.MIN_VALUE, constVal, false, true),
                        TiRange.createLongRange(constVal, Long.MAX_VALUE, true, false)
                );
            }
        }
        throw new TiClientInternalException("Expression cannot be cast ot KeyRange");
    }
}
