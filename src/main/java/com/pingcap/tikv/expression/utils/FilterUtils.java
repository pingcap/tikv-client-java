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
import com.google.common.collect.Lists;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

// TODO: This utility class need a total redesign
// for now it's a brainless translation from TiDB's code
public class FilterUtils {
    public static class ScanPlan {
        public final List<TiExpr>           filters;
        public final Expr                   expr;
        public final List<TiRange<Long>>    keyRange;

        public ScanPlan(List<TiExpr> filters,
                        Expr pushdownFilters,
                        List<TiRange<Long>> keyRange) {
            this.filters = filters;
            this.expr = pushdownFilters;
            this.keyRange = keyRange;
        }
    }

    public static ScanPlan buildScan(List<TiExpr> conditions, TiTableInfo table) {
        Pair<List<TiExpr>, List<TiExpr>> resultPair =
                detachTableScanConditions(conditions, table);

        Pair<Expr, List<TiExpr>> toProtoResult = filterToProto(resultPair.second);
        List<TiRange<Long>> ranges = buildTableRange(resultPair.first);
        return new ScanPlan(resultPair.second, toProtoResult.first, ranges);
    }

    public static Pair<Expr, List<TiExpr>> filterToProto(List<TiExpr> conditions) {
        ImmutableList.Builder<TiExpr> remainingConds = ImmutableList.builder();
        TiExpr rootExpr = null;
        // Conditions are in CNF
        for (TiExpr cond : conditions) {
            if (cond.isSupportedExpr()) {
                if (rootExpr == null) {
                    rootExpr = cond;
                } else {
                    rootExpr = new And(rootExpr, cond);
                }
            } else {
                remainingConds.add(cond);
            }
        }
        return Pair.create(rootExpr.toProto(), remainingConds.build());
    }

    public static boolean conditionMatchesIndexColumn(TiExpr expr, TiIndexColumn col) {
        if (!(expr instanceof Equal)) {
            return false;
        }
        Equal eq = (Equal)expr;
        TiColumnRef ref = null;
        if (eq.getArg(0) instanceof TiConstant &&
                eq.getArg(1) instanceof TiColumnRef) {
            ref = (TiColumnRef)eq.getArg(1);
        } else if (eq.getArg(1) instanceof TiConstant &&
                eq.getArg(0) instanceof TiColumnRef) {
            ref = (TiColumnRef)eq.getArg(1);
        } else {
            return false;
        }
        if (col.matchName(ref.getName())) {
            return true;
        }
        return false;
    }

    public static Pair<List<TiExpr>, List<TiExpr>> // access point conditions, remain conditions
    detachIndexScanConditions(List<TiExpr> conditions, TiIndexInfo index) {
        // TODO: Make sure IN expr is normalized to EQ list
        // 1. Generate access point based on equal conditions
        // 2. Cut access point condition if index is not continuous
        // 3. Push back prefix index conditions since prefix index retrieve more result than needed
        // 4. For remaining indexes (since access conditions consume some index, and they will
        // not be used in filter push down later), find continuous matching index until first unmatched
        // 5. Push back index related filter if prefix index, for remaining filters
        // Equal conditions needs to be process first according to index sequence
        List<TiExpr> accessConditions = new ArrayList<>();
        List<TiExpr> filterConditions = new ArrayList<>();
        for (int i = 0; i < index.getIndexColumns().size(); i++) {
            TiIndexColumn col = index.getIndexColumns().get(i);
            boolean found = false;
            for (TiExpr cond : conditions) {
                if (conditionMatchesIndexColumn(cond, col)) {
                    accessConditions.add(cond);
                    if (col.isPrefixIndex()) {
                        filterConditions.add(cond);
                    }
                }
                found = true;
            }
            if (!found) {
                conditions = Lists.newArrayList(TiFluentIterable
                        .from(conditions)
                        .filter(c -> !accessConditions.contains(c))
                );
                ConditionChecker checker = new ConditionChecker(index, i);
                for (TiExpr cond : conditions) {
                    if (!checker.check(cond)) {
                        filterConditions.add(cond);
                        continue;
                    }
                    accessConditions.add(cond);
                    if (col.isPrefixIndex()) {
                        filterConditions.add(cond);
                    }
                }
                break;
            }
        }
        return Pair.create(accessConditions, filterConditions);
    }

    public static Pair<List<TiExpr>, List<TiExpr>> // access point conditions, remain conditions
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

    public static List<TiRange<Long>> buildTableRange(List<TiExpr> accessConditions) {
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
        return ImmutableList.of(TiRange.FULL_LONG_RANGE);
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
