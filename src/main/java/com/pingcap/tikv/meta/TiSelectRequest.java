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

package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.predicates.PredicateUtils.extractColumnRefFromExpr;
import static com.pingcap.tikv.predicates.PredicateUtils.mergeCNFExpressions;
import static java.util.Objects.requireNonNull;


public class TiSelectRequest implements Serializable {
    public enum TruncateMode {
        IgnoreTruncation(0x1),
        TruncationAsWarning(0x2);

        private final long mask;

        TruncateMode(long mask) {
            this.mask = mask;
        }

        public long mask(long flags) {
            return flags | mask;
        }
    }

    private static final KeyRange FULL_RANGE = KeyRange
                                                    .newBuilder()
                                                    .setStart(ByteString.EMPTY)
                                                    .setEnd(ByteString.EMPTY)
                                                    .build();

    private TiTableInfo tableInfo;
    private TiIndexInfo indexInfo;
    private final List<TiExpr> fields = new ArrayList<>();
    private final List<TiExpr> where = new ArrayList<>();
    private final List<TiByItem> groupByItems = new ArrayList<>();
    private final List<TiByItem> orderByItems = new ArrayList<>();
    private final List<TiExpr> aggregates = new ArrayList<>();
    private final List<KeyRange> keyRanges = new ArrayList<>();

    private int limit;
    private int timeZoneOffset;
    private long flags;
    private long startTs;
    private TiExpr having;
    private boolean distinct;

    public void bind() {
        fields.forEach(expr -> expr.bind(tableInfo));
        where.forEach(expr -> expr.bind(tableInfo));
        groupByItems.forEach(item -> item.getExpr().bind(tableInfo));
        orderByItems.forEach(item -> item.getExpr().bind(tableInfo));
        aggregates.forEach(expr -> expr.bind(tableInfo));
        if (having != null) {
            having.bind(tableInfo);
        }
    }

    public SelectRequest buildAsIndexScan() {
        checkArgument(startTs != 0, "timestamp is 0");
        SelectRequest.Builder builder = SelectRequest.newBuilder();
        if (indexInfo == null) {
            throw new TiClientInternalException("Index is empty for index scan");
        }
        builder.setIndexInfo(indexInfo.toProto(tableInfo));
        builder.setFlags(flags);

        builder.setTimeZoneOffset(timeZoneOffset);
        builder.setStartTs(startTs);
        return builder.build();
    }

    public SelectRequest build() {
        checkArgument(startTs != 0, "timestamp is 0");
        SelectRequest.Builder builder = SelectRequest.newBuilder();
        // TODO: add optimize later
        // Optimize merge groupBy
        fields.forEach(expr -> builder.addFields(expr.toProto()));

        Set<TiColumnRef> usedColumnRef = new HashSet<>();
        if (!fields.isEmpty()) {
            for (TiExpr field : fields) {
                builder.addFields(field.toProto());
                usedColumnRef.addAll(extractColumnRefFromExpr(field));
            }
        }

        for (TiByItem item : groupByItems) {
            builder.addGroupBy(item.toProto());
            usedColumnRef.addAll(extractColumnRefFromExpr(item.getExpr()));
        }

        for (TiByItem item : orderByItems) {
            builder.addOrderBy(item.toProto());
        }

        for (TiExpr agg : aggregates) {
            builder.addAggregates(agg.toProto());
        }

        List<TiColumnInfo> columns = usedColumnRef
                                    .stream()
                                    .map(colRef -> colRef.getColumnInfo())
                                    .collect(Collectors.toList());

        TiTableInfo filteredTable = new TiTableInfo(
                tableInfo.getId(),
                CIStr.newCIStr(tableInfo.getName()),
                tableInfo.getCharset(),
                tableInfo.getCollate(),
                tableInfo.isPkHandle(),
                columns,
                tableInfo.getIndices(),
                tableInfo.getComment(),
                tableInfo.getAutoIncId(),
                tableInfo.getMaxColumnId(),
                tableInfo.getMaxIndexId(),
                tableInfo.getOldSchemaId()
        );

        TiExpr whereExpr = mergeCNFExpressions(where);
        if (whereExpr != null) {
            builder.setWhere(whereExpr.toProto());
        }

        builder.setTableInfo(filteredTable.toProto());
        builder.setFlags(flags);

        builder.setTimeZoneOffset(timeZoneOffset);
        builder.setStartTs(startTs);
        return builder.build();
    }

    public TiSelectRequest setTableInfo(TiTableInfo tableInfo) {
        this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
        return this;
    }

    public TiTableInfo getTableInfo() {
        return this.tableInfo;
    }

    public TiSelectRequest setIndexInfo(TiIndexInfo indexInfo) {
        this.indexInfo = requireNonNull(indexInfo, "indexInfo is null");
        return this;
    }

    public TiIndexInfo getIndexInfo() {
        return indexInfo;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * add limit clause to select query.
     *
     * @param limit is just a integer.
     * @return a SelectBuilder
     */
    public TiSelectRequest setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * set timezone offset
     *
     * @param timeZoneOffset timezone offset
     * @return a TiSelectRequest
     */
    public TiSelectRequest setTimeZoneOffset(int timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
        return this;
    }

    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    /**
     * set truncate mode
     *
     * @param mode truncate mode
     * @return a TiSelectRequest
     */
    public TiSelectRequest setTruncateMode(TruncateMode mode) {
        flags = requireNonNull(mode, "mode is null").mask(flags);
        return this;
    }

    @VisibleForTesting
    public long getFlags() {
        return flags;
    }

    /**
     * set start timestamp for the transaction
     *
     * @param startTs timestamp
     * @return a TiSelectRequest
     */
    public TiSelectRequest setStartTs(long startTs) {
        this.startTs = startTs;
        return this;
    }

    public long getStartTs() {
        return startTs;
    }

    /**
     * set having clause to select query
     *
     * @param having is a expression represents Having
     * @return a TiSelectRequest
     */
    public TiSelectRequest setHaving(TiExpr having) {
        this.having = requireNonNull(having, "having is null");
        return this;
    }

    public TiSelectRequest setDistinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public boolean isDistinct() {
        return distinct;
    }

    /**
     * add aggregate function to select query
     *
     * @param expr is a TiUnaryFunction expression.
     * @return a SelectBuilder
     */
    public TiSelectRequest addAggregate(TiExpr expr) {
        aggregates.add(requireNonNull(expr, "aggregation expr is null"));
        return this;
    }

    public List<TiExpr> getAggregates() {
        return aggregates;
    }

    /**
     * add a order by clause to select query.
     *
     * @param byItem is a TiByItem.
     * @return a SelectBuilder
     */
    public TiSelectRequest addOrderByItem(TiByItem byItem) {
        orderByItems.add(requireNonNull(byItem, "byItem is null"));
        return this;
    }

    public List<TiByItem> getOrderByItems() {
        return orderByItems;
    }

    /**
     * add a group by clause to select query
     *
     * @param byItem is a TiByItem
     * @return a SelectBuilder
     */
    public TiSelectRequest addGroupByItem(TiByItem byItem) {
        groupByItems.add(requireNonNull(byItem, "byItem is null"));
        return this;
    }

    public List<TiByItem> getGroupByItems() {
        return groupByItems;
    }

    /**
     * field is not support in TiDB yet, but we still implement this
     * in case of TiDB support it in future.
     * The usage of this function will be simple query such as select c1 from t.
     *
     * @param expr expr is a TiExpr. It is usually TiColumnRef.
     */
    public TiSelectRequest addField(TiExpr expr) {
        fields.add(requireNonNull(expr, "Field expr is null"));
        return this;
    }

    public List<TiExpr> getFields() {
        return fields;
    }

    /**
     * set key range of scan
     *
     * @param ranges key range of scan
     */
    public TiSelectRequest addRanges(List<KeyRange> ranges) {
        keyRanges.addAll(requireNonNull(ranges, "KeyRange is null"));
        return this;
    }

    public TiSelectRequest resetRanges(List<KeyRange> ranges) {
        keyRanges.clear();
        keyRanges.addAll(ranges);
        return this;
    }

    public List<KeyRange> getRanges() {
        if (keyRanges.isEmpty()) {
            return ImmutableList.of(FULL_RANGE);
        }
        return keyRanges;
    }

    public TiSelectRequest addWhere(TiExpr where) {
        this.where.add(requireNonNull(where, "where expr is null"));
        return this;
    }

    public List<TiExpr> getWhere() {
        return where;
    }
}
