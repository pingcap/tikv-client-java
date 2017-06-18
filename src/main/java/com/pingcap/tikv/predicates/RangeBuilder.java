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

package com.pingcap.tikv.predicates;


import com.google.common.collect.*;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.predicates.AccessConditionNormalizer.NormalizedCondition;
import com.pingcap.tikv.types.FieldType;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RangeBuilder {
    private static Object checkAndExtractConst(TiConstant constVal, FieldType type) {
        if (type.needCast(constVal.getValue())) {
            throw new TiClientInternalException("Casting not allowed: " + constVal + " to type " + type);
        }
        return constVal.getValue();
    }

    /**
     * Turn access conditions into list of points
     * All access conditions are bound to single key
     * @param accessPoints expressions that convertible to access points
     * @param types index column types
     * @return access points for each index
     */
    public List<IndexRange> exprsToPoints(List<TiExpr> accessPoints,
                                            List<FieldType> types) {
        requireNonNull(accessPoints, "accessPoints cannot be null");
        requireNonNull(types, "Types cannot be null");
        checkArgument(accessPoints.size() == types.size(),
                      "Access points size and type size mismatches");

        List<IndexRange> irs = new ArrayList<>();
        for (int i = 0; i < accessPoints.size(); i++) {
            TiExpr func = accessPoints.get(i);
            FieldType type = types.get(i);
            try {
                List<Object> points = exprToPoints(func, type);
                irs = IndexRange.appendPointsForSingleCondition(irs, points, type);
            } catch (Exception e) {
                throw new TiClientInternalException("Error converting access points" + func);
            }
        }
        return irs;
    }

    public static class IndexRange {
        private List<Object>        accessPoints;
        private List<FieldType>     types;
        private Range               range;
        private FieldType           rangeType;

        public IndexRange(List<Object> accessPoints, List<FieldType> types, Range range, FieldType rangeType) {
            this.accessPoints = accessPoints;
            this.types = types;
            this.range = range;
            this.rangeType = rangeType;
        }

        public IndexRange(List<Object> accessPoints, List<FieldType> types) {
            this.accessPoints = accessPoints;
            this.types = types;
            this.range = null;
        }

        public static List<IndexRange> appendPointsForSingleCondition(
                                                    List<IndexRange> indexRanges,
                                                    List<Object> points,
                                                    FieldType type) {
            requireNonNull(indexRanges);
            requireNonNull(points);
            requireNonNull(type);

            List<IndexRange> resultRanges = new ArrayList<>();
            if (indexRanges.size() == 0) {
                resultRanges.add(new IndexRange(points, ImmutableList.of(type)));
                return resultRanges;
            }

            for (IndexRange ir : indexRanges) {
                resultRanges.addAll(ir.appendPoint(points, type));
            }
            return resultRanges;
        }

        private List<IndexRange> appendPoint(List<Object> points, FieldType type) {
            List<IndexRange> result = new ArrayList<>();
            for (Object p : points) {
                ImmutableList.Builder<Object> newAccessPoints =
                                                ImmutableList.builder()
                                                        .addAll(accessPoints)
                                                        .add(p);

                ImmutableList.Builder<FieldType> newTypes =
                                                ImmutableList.<FieldType>builder()
                                                        .addAll(types)
                                                        .add(type);
                newAccessPoints.add(p);
                newTypes.add(type);

                result.add(new IndexRange(newAccessPoints.build(), newTypes.build()));
            }
            return result;
        }

        public static List<IndexRange> appendRanges(List<IndexRange> indexRanges, List<Range> ranges) {
            requireNonNull(indexRanges);
            requireNonNull(ranges);
            List<IndexRange> resultRanges = new ArrayList<>();
            for (IndexRange ir : indexRanges) {
                for (Range r : ranges) {
                    resultRanges.add(new IndexRange(ir.getAccessPoints(), ir.getTypes(), r, ir.getRangeType()));
                }
            }
            return resultRanges;
        }

        public List<Object> getAccessPoints() {
            return accessPoints;
        }

        public Range getRange() {
            return range;
        }

        public List<FieldType> getTypes() {
            return types;
        }

        public FieldType getRangeType() {
            return rangeType;
        }
    }

    private List<Object> exprToPoints(TiExpr expr, FieldType type) {
        try {
            if (expr instanceof Or) {
                Or orExpr = (Or) expr;
                return ImmutableList
                        .builder()
                        .addAll(exprToPoints(orExpr.getArg(0), type))
                        .addAll(exprToPoints(orExpr.getArg(1), type))
                        .build();
            }
            checkArgument(expr instanceof Equal || expr instanceof In,
                       "Only In and Equal can convert to points");
            TiFunctionExpression func = (TiFunctionExpression)expr;
            NormalizedCondition cond = AccessConditionNormalizer.normalize(func);
            ImmutableList.Builder<Object> result = ImmutableList.builder();
            cond.constantVals.forEach(
                    constVal -> result.add(checkAndExtractConst(constVal, type))
            );
            return result.build();
        } catch (Exception e) {
            throw new TiClientInternalException("Failed to convert expr to points: " + expr, e);
        }
    }

    /**
     * Turn CNF filters into range
     * @param accessConditions filters in CNF list
     * @param type index column type
     * @return access ranges
     */
    public List<Range> exprToRanges(List<TiExpr> accessConditions, FieldType type) {
        RangeSet ranges = TreeRangeSet.create();
        ranges.add(Range.all());
        for (TiExpr expr : accessConditions) {
            NormalizedCondition cond = AccessConditionNormalizer.normalize(expr);
            TiConstant constVal = cond.constantVals.get(0);
            Comparable<?> comparableVal = Comparables.wrap(constVal.getValue());
            Range r = null;
            if (expr instanceof GreaterThan) {
                ranges = ranges.subRangeSet(Range.greaterThan(comparableVal));
            } else if (expr instanceof GreaterEqual) {
                ranges.subRangeSet(Range.atLeast(comparableVal));
            } else if (expr instanceof LessThan) {
                ranges.subRangeSet(Range.lessThan(comparableVal));
            } else if (expr instanceof LessEqual) {
                ranges = ranges.subRangeSet(Range.atMost(comparableVal));
            } else if (expr instanceof Equal) {
                ranges = ranges.subRangeSet(Range.singleton(comparableVal));
            } else if (expr instanceof NotEqual) {
                RangeSet left = ranges.subRangeSet(Range.lessThan(comparableVal));
                RangeSet right = ranges.subRangeSet(Range.greaterThan(comparableVal));
                ranges = TreeRangeSet.create(left);
                ranges.addAll(right);
            } else {
                throw new TiClientInternalException("Unsupported conversion to Range " + expr.getClass().getSimpleName());
            }
        }
        return ImmutableList.copyOf(ranges.asRanges());
    }
}
