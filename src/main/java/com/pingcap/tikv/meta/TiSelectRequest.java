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
import com.pingcap.tidb.tipb.IndexInfo;
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
    private IndexInfo indexInfo;
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
        // add optimize later
        // Optimize merge groupBy
        this.fields.forEach(expr -> this.builder.addFields(expr.toProto()));
        if (!this.fields.isEmpty()) {
            // convert fields type to set for later usage.
            Set<String> fieldSet = ImmutableSet.copyOf(TiFluentIterable.from(this.fields).transform(
                    expr -> ((TiColumnRef) expr).getName()
            ));
            // solve
            List<TiColumnInfo> colToRemove = new ArrayList<>();
            this.tableInfo.getColumns().forEach(
                    col -> {
                        // remove column from table if such column is not in fields
                        if (!fieldSet.contains(col.getName())) {
                            colToRemove.add(col);
                        }
                    }
            );

            this.tableInfo.getColumns().removeAll(colToRemove);
        }
        this.builder.setWhere(PredicateUtils.mergeCNFExpressions(this.where).toProto());
        this.groupBys.forEach(expr -> this.builder.addGroupBy(expr.toProto()));
        this.orderBys.forEach(expr -> this.builder.addOrderBy(expr.toProto()));
        this.aggregates.forEach(expr -> this.builder.addAggregates(expr.toProto()));
        this.builder.setFlags(flags);
        this.builder.setTableInfo(tableInfo.toProto());
        this.builder.setTimeZoneOffset(timeZoneOffset);
        this.builder.setStartTs(startTs);
        this.builder.addAllRanges(keyRanges);
        return this.builder.build();
    }
}
