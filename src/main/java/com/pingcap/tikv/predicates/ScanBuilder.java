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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;

import java.util.ArrayList;
import java.util.List;

public class ScanBuilder {
    public void buildScan(List<TiExpr> conditions, TiIndexInfo index, TiTableInfo table) {
        IndexMatchingResult result = extractConditions(conditions, table, index);
        RangeBuilder rangeBuilder = new RangeBuilder();
        List<IndexRange> irs =
                rangeBuilder.exprsToPoints(result.accessPoints, result.accessPointsTypes);

        List<Range> ranges = rangeBuilder.exprToRanges(result.accessConditions, result.rangeType);

        irs = IndexRange.appendRanges(irs, ranges, result.rangeType);

        List<KeyRange> keyRanges = buildKeyRange(irs);
    }

    private List<KeyRange> buildKeyRange(List<IndexRange> indexRanges) {
        List<Object> keyPrefix = new ArrayList<>();
        for (IndexRange ir : indexRanges) {
            // ir.getAccessPoints()
        }
        return null;
    }

    public static class IndexMatchingResult {
        public final List<TiExpr>       residualConditions;
        public final List<TiExpr>       accessPoints;
        public final List<DataType>     accessPointsTypes;
        public final List<TiExpr>       accessConditions;
        public final DataType           rangeType;

        public IndexMatchingResult(List<TiExpr> residualConditions,
                                   List<TiExpr> accessPoints,
                                   List<DataType> accessPointsTypes,
                                   List<TiExpr> accessConditions,
                                   DataType rangeType) {
            this.residualConditions = residualConditions;
            this.accessPoints = accessPoints;
            this.accessPointsTypes = accessPointsTypes;
            this.accessConditions = accessConditions;
            this.rangeType = rangeType;
        }
    }

    @VisibleForTesting
    public IndexMatchingResult extractConditions(List<TiExpr> conditions, TiTableInfo table, TiIndexInfo index) {
        // 1. Generate access point based on equal conditions
        // 2. Cut access point condition if index is not continuous
        // 3. Push back prefix index conditions since prefix index retrieve more result than needed
        // 4. For remaining indexes (since access conditions consume some index, and they will
        // not be used in filter push down later), find continuous matching index until first unmatched
        // 5. Push back index related filter if prefix index, for remaining filters
        // Equal conditions needs to be process first according to index sequence
        List<TiExpr> accessPoints = new ArrayList<>();
        List<DataType> accessPointTypes = new ArrayList<>();
        List<TiExpr> residualConditions = new ArrayList<>();
        List<TiExpr> accessConditions = new ArrayList<>();
        DataType accessConditionType = null;

        for (int i = 0; i < index.getIndexColumns().size(); i++) {
            // for each index column try matches an equal condition
            // and push remaining back
            // TODO: if more than one equal conditions match an
            // index, it likely yields nothing. Maybe a check needed
            // to simplify it to a false condition
            TiIndexColumn col = index.getIndexColumns().get(i);
            IndexMatcher matcher = new IndexMatcher(col, true);
            boolean found = false;
            // For first prefix index encountered, it equals to a range
            // and we cannot push equal conditions further
            if (!col.isPrefixIndex()) {
                for (TiExpr cond : conditions) {
                    if (matcher.match(cond)) {
                        accessPoints.add(cond);
                        TiColumnInfo tiColumnInfo = table.getColumns().get(col.getOffset());
                        accessPointTypes.add(tiColumnInfo.getType());
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                // For first "broken index chain piece"
                // search for a matching range condition
                matcher = new IndexMatcher(col, false);
                for (TiExpr cond : conditions) {
                    if (matcher.match(cond)) {
                        accessConditions.add(cond);
                        TiColumnInfo tiColumnInfo = table.getColumns().get(col.getOffset());
                        accessConditionType = tiColumnInfo.getType();
                    }
                    if (col.isPrefixIndex()) {
                        residualConditions.add(cond);
                    }
                }
                break;
            }
        }

        return new IndexMatchingResult(residualConditions,
                                        accessPoints,
                                        accessPointTypes,
                                        accessConditions,
                                        accessConditionType);
    }
}
