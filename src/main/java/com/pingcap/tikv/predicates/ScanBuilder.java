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
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// TODO: Rethink value binding part since we abstract away datum of TiDB
public class ScanBuilder {
    public static class ScanPlan {
        public ScanPlan(List<KeyRange> keyRanges, List<TiExpr> filters) {
            this.filters = filters;
            this.keyRanges = keyRanges;
        }

        private final List<KeyRange> keyRanges;
        private final List<TiExpr> filters;

        public List<KeyRange> getKeyRanges() {
            return keyRanges;
        }

        public List<TiExpr> getFilters() {
            return filters;
        }
    }

    public ScanPlan buildScan(List<TiExpr> conditions, TiIndexInfo index, TiTableInfo table) {
        requireNonNull(table, "Table cannot be null to encoding keyRange");
        requireNonNull(index, "Index cannot be null to encoding keyRange");
        requireNonNull(conditions, "conditions cannot be null to encoding keyRange");

        IndexMatchingResult result = extractConditions(conditions, table, index);
        List<IndexRange> irs = RangeBuilder.exprsToIndexRanges(result.accessPoints, result.accessPointsTypes,
                                                               result.accessConditions, result.rangeType);

        List<KeyRange> keyRanges;
        if (index.isFakePrimaryKey()) {
            keyRanges = buildTableScanKeyRange(table, irs);
        } else {
            keyRanges = buildIndexScanKeyRange(table, index, irs);
        }

        return new ScanPlan(keyRanges, result.residualConditions);
    }

    private List<KeyRange> buildTableScanKeyRange(TiTableInfo       table,
                                                        List<IndexRange>  indexRanges) {
        checkArgument(table.isPkHandle());
        requireNonNull(table, "Table cannot be null to encoding keyRange");
        requireNonNull(indexRanges, "indexRanges cannot be null to encoding keyRange");

        List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
        for (IndexRange ir : indexRanges) {
            ByteString startKey;
            ByteString endKey;
            if (ir.hasAccessPoints()) {
                checkArgument(!ir.hasRange(),
                        "Table scan must have one and only one access condition / point");

                Object v = ir.getAccessPoints().get(0);
                checkArgument(v instanceof Long, "Table scan key range must be long value");
                DataType type = ir.getTypes().get(0);
                checkArgument(type instanceof IntegerType, "Table scan key range must be long value");
                startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), (long)v);
                endKey = ByteString.copyFrom(KeyUtils.prefixNext(startKey.toByteArray()));
            } else if (ir.hasRange()) {
                checkArgument(!ir.hasAccessPoints(),
                        "Table scan must have one and only one access condition / point");
                CodecDataOutput cdo = new CodecDataOutput();
                Range r = ir.getRange();
                DataType type = ir.getRangeType();
                checkArgument(type instanceof IntegerType, "Table scan key range must be long value");

                if (!r.hasLowerBound()) {
                    // -INF
                    // TODO: Domain and encoding should be further encapsulated
                    startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MIN_VALUE);
                } else {
                    // Comparision with null should be filtered since it yields unknown always
                    Object lb = r.lowerEndpoint();
                    checkArgument(lb instanceof Long, "Table scan key range must be long value");
                    long lVal = (long)lb;
                    if (r.lowerBoundType().equals(BoundType.OPEN)) {
                        // TODO: Need push back?
                        if (lVal != Long.MAX_VALUE) {
                            lVal++;
                        }
                    }
                    startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), lVal);
                }

                if (!r.hasUpperBound()) {
                    // INF
                    endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), Long.MAX_VALUE);
                } else {
                    Object ub = r.upperEndpoint();
                    checkArgument(ub instanceof Long, "Table scan key range must be long value");
                    long lVal = (long)ub;
                    if (r.upperBoundType().equals(BoundType.CLOSED)) {
                        if (lVal != Long.MAX_VALUE) {
                            lVal++;
                        }
                    }
                    endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), lVal);
                }
            } else {
                throw new TiClientInternalException("Empty access conditions");
            }

            ranges.add(
                    KeyRange.newBuilder()
                            .setLow(startKey)
                            .setHigh(endKey)
                            .build()
            );
        }

        return ranges;
    }

    private List<KeyRange> buildIndexScanKeyRange(TiTableInfo           table,
                                                  TiIndexInfo           index,
                                                  List<IndexRange>      indexRanges) {
        requireNonNull(table, "Table cannot be null to encoding keyRange");
        requireNonNull(index, "Index cannot be null to encoding keyRange");
        requireNonNull(index, "indexRanges cannot be null to encoding keyRange");

        List<KeyRange> ranges = new ArrayList<>(indexRanges.size());

        for (IndexRange ir : indexRanges) {
            CodecDataOutput cdo = new CodecDataOutput();
            List<Object> values = ir.getAccessPoints();
            List<DataType> types = ir.getTypes();
            for (int i = 0; i < values.size(); i++) {
                Object v = values.get(i);
                DataType t = types.get(i);
                t.encode(cdo, DataType.EncodeType.KEY, v);
            }

            byte[] pointsData = cdo.toBytes();

            cdo.reset();
            Range r = ir.getRange();
            DataType type = ir.getRangeType();
            byte[] lKey;
            if (!r.hasLowerBound()) {
                // -INF
                type.encodeMinValue(cdo);
                lKey = cdo.toBytes();
            } else {
                Object lb = r.lowerEndpoint();
                type.encode(cdo, DataType.EncodeType.KEY, lb);
                lKey = cdo.toBytes();
                if (r.lowerBoundType().equals(BoundType.OPEN)) {
                    lKey = KeyUtils.prefixNext(lKey);
                }
            }

            byte[] uKey;
            cdo.reset();
            if (!r.hasUpperBound()) {
                // INF
                type.encodeMaxValue(cdo);
                uKey = cdo.toBytes();
            } else {
                Object ub = r.upperEndpoint();
                type.encode(cdo, DataType.EncodeType.KEY, ub);
                uKey = cdo.toBytes();
                if (r.upperBoundType().equals(BoundType.CLOSED)) {
                    uKey = KeyUtils.prefixNext(lKey);
                }
            }

            cdo.reset();
            TableCodec.writeIndexSeekKey(cdo, table.getId(), index.getId(), pointsData, lKey);

            ByteString lbsKey = ByteString
                                    .copyFrom(cdo.toBytes());

            cdo.reset();
            TableCodec.writeIndexSeekKey(cdo, table.getId(), index.getId(), pointsData, uKey);
            ByteString ubsKey = ByteString
                                    .copyFrom(cdo.toBytes());

            ranges.add(
                    KeyRange.newBuilder()
                    .setLow(lbsKey)
                    .setHigh(ubsKey)
                    .build()
            );
        }

        return ranges;
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
    public static IndexMatchingResult extractConditions(List<TiExpr> conditions, TiTableInfo table, TiIndexInfo index) {
        // 0. Different than TiDB implementation, here logic has been unified for TableScan and IndexScan by
        // adding fake index on clustered table's pk
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

        Set<TiExpr> visited = new HashSet<>();
        IndexMatchingLoop:
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
            for (TiExpr cond : conditions) {
                if (visited.contains(cond)) continue;
                if (matcher.match(cond)) {
                    accessPoints.add(cond);
                    TiColumnInfo tiColumnInfo = table.getColumns().get(col.getOffset());
                    accessPointTypes.add(tiColumnInfo.getType());
                    if (col.isPrefixIndex()) {
                        residualConditions.add(cond);
                        break IndexMatchingLoop;
                    }
                    visited.add(cond);
                    found = true;
                    break;
                }
            }
            if (!found) {
                // For first "broken index chain piece"
                // search for a matching range condition
                matcher = new IndexMatcher(col, false);
                for (TiExpr cond : conditions) {
                    if (visited.contains(cond)) continue;
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

        // push remaining back
        conditions.stream().filter(
                cond -> !accessPoints.contains(cond) &&
                        !accessConditions.contains(cond) &&
                        !residualConditions.contains(cond)
        ).forEach(residualConditions::add);

        return new IndexMatchingResult(residualConditions,
                                        accessPoints,
                                        accessPointTypes,
                                        accessConditions,
                                        accessConditionType);
    }
}
