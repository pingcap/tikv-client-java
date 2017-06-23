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
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Data
public class TiSelectRequest {
    private final SelectRequest.Builder builder;
    private TiTableInfo tableInfo;
    private TiIndexInfo indexInfo;
    private final List<TiExpr> fields = new ArrayList<>();
    private final List<TiExpr> where = new ArrayList<>();
    private final List<TiByItem> groupBys = new ArrayList<>();
    private final List<TiByItem> orderBys = new ArrayList<>();
    private final List<TiExpr>   aggregates = new ArrayList<>();
    private final List<KeyRange> keyRanges = new ArrayList<>();
    private int limit;
    private int timeZoneOffset;
    private long flags;
    private long startTs;
    private TiExpr having;
    private boolean distinct;

    public SelectRequest toProto() {
        return build();
    }

    public SelectRequest build() {
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
            columns.forEach(
                    col -> {
                        // remove column from table if such column is not in fields
                        if (!fieldSet.contains(col.getName())) {
                            colToRemove.add(col);
                        }
                    }
            );
            // TODO: Add a clone before modify
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
            builder.setWhere(PredicateUtils.mergeCNFExpressions(where).toProto());
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
}
