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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.util.TiFluentIterable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

    private TiTableInfo tableInfo;
    private TiIndexInfo indexInfo;
    private final List<TiExpr> fields = new ArrayList<>();
    private final List<TiExpr> where = new ArrayList<>();
    private final List<TiByItem> groupBys = new ArrayList<>();
    private final List<TiByItem> orderBys = new ArrayList<>();
    private final List<TiExpr> aggregates = new ArrayList<>();

    private int limit;
    private int timeZoneOffset;
    private long flags;
    private long startTs;
    private TiExpr having;
    private boolean distinct;
    private List<KeyRange> keyRanges;

    public SelectRequest build() {
        SelectRequest.Builder builder = SelectRequest.newBuilder();
        // TODO: add optimize later
        // Optimize merge groupBy
        fields.forEach(expr -> builder.addFields(expr.toProto()));

        if (indexInfo != null) {
            builder.setIndexInfo(indexInfo.toProto(tableInfo));
        }
        TiTableInfo filteredTable = tableInfo;
        if (!fields.isEmpty()) {
            List<TiColumnInfo> columns = Lists.newArrayList(tableInfo.getColumns());
            // convert fields type to set for later usage.
            Set<String> fieldSet = ImmutableSet.copyOf(TiFluentIterable.from(fields).transform(
                    expr -> ((TiColumnRef) expr).getName()
            ));
            // solve
            List<TiColumnInfo> colToRemove = new ArrayList<>();
            for (TiColumnInfo column : columns) {
                // remove column from table if such column is not in fields
                if (!fieldSet.contains(column.getName())) {
                    colToRemove.add(column);
                }
            }

            columns.removeAll(colToRemove);
            filteredTable = new TiTableInfo(
                    tableInfo.getId(),
                    CIStr.newCIStr(tableInfo.getName()),
                    tableInfo.getCharset(),
                    tableInfo.getCollate(),
                    tableInfo.isPkHandle(),
                    tableInfo.getColumns(),
                    tableInfo.getIndices(),
                    tableInfo.getComment(),
                    tableInfo.getAutoIncId(),
                    tableInfo.getMaxColumnId(),
                    tableInfo.getMaxIndexId(),
                    tableInfo.getOldSchemaId()
            );
        }
        if (indexInfo == null) {
            TiExpr whereExpr = PredicateUtils.mergeCNFExpressions(where);
            if (whereExpr != null) {
                builder.setWhere(whereExpr.toProto());
            }
            groupBys.forEach(expr -> builder.addGroupBy(expr.toProto()));
            orderBys.forEach(expr -> builder.addOrderBy(expr.toProto()));
            aggregates.forEach(expr -> builder.addAggregates(expr.toProto()));
            builder.setTableInfo(filteredTable.toProto());
        }
        builder.setFlags(flags);

        builder.setTimeZoneOffset(timeZoneOffset);
        builder.setStartTs(startTs);
        //builder.addAllRanges(keyRanges);
        return builder.build();
    }

    public TiSelectRequest setTableInfo(TiTableInfo tableInfo) {
        this.tableInfo = tableInfo;
        return this;
    }

    public TiSelectRequest setIndexInfo(TiIndexInfo indexInfo) {
        this.indexInfo = indexInfo;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * add limit clause to select query.
     * @param limit is just a integer.
     * @return a SelectBuilder
     */
    public TiSelectRequest setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * set timezone offset
     * @param timeZoneOffset timezone offset
     * @return a TiSelectRequest
     */
    public TiSelectRequest setTimeZoneOffset(int timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
        return this;
    }

    /**
     * set truncate mode
     * @param mode truncate mode
     * @return a TiSelectRequest
     */
    public TiSelectRequest setTruncateMode(TruncateMode mode) {
        flags = requireNonNull(mode, "mode is null").mask(flags);
        return this;
    }

    /**
     * set start timestamp for the transaction
     * @param startTs timestamp
     * @return a TiSelectRequest
     */
    public TiSelectRequest setStartTs(long startTs) {
        this.startTs = startTs;
        return this;
    }

    /**
     * set having clause to select query
     * @param having is a expression represents Having
     * @return a TiSelectRequest
     */
    public TiSelectRequest setHaving(TiExpr having) {
        this.having = requireNonNull(having, "having is null");
        return this;
    }

    public TiSelectRequest setDistinct(boolean distinct) {
        distinct = distinct;
        return this;
    }

    /**
     * add aggregate function to select query
     *
     * @param expr is a TiUnaryFunction expression.
     * @return a SelectBuilder
     */
    public TiSelectRequest addAggregate(TiExpr expr) {
        aggregates.add(expr);
        return this;
    }

    public List<TiExpr> getAggregates() {
        return aggregates;
    }

    /**
     * add a order by clause to select query.
     * @param byItem is a TiByItem.
     * @return a SelectBuilder
     */
    public TiSelectRequest addOrderBy(TiByItem byItem) {
        orderBys.add(byItem);
        return this;
    }

    /**
     * add a group by clause to select query
     * @param byItem is a TiByItem
     * @return a SelectBuilder
     */
    public TiSelectRequest addGroupBy(TiByItem byItem) {
        groupBys.add(byItem);
        return this;
    }

    public List<TiByItem> getGroupBys() {
        return groupBys;
    }

    /**
     * field is not support in TiDB yet, but we still implement this
     * in case of TiDB support it in future.
     * The usage of this function will be simple query such as select c1 from t.
     *
     * @param expr expr is a TiExpr. It is usually TiColumnRef.
     */
    public TiSelectRequest addField(TiExpr expr) {
        fields.add(expr);
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
        keyRanges.addAll(ranges);
        return this;
    }

    public List<KeyRange> getRanges() {
        return keyRanges;
    }

    public TiSelectRequest addWhere(TiExpr where) {
        this.where.add(where);
        return this;
    }
}
