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

import com.pingcap.tidb.tipb.IndexInfo;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.TableInfo;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TiSelectRequest {
    private final SelectRequest.Builder builder = SelectRequest.newBuilder();
    private TiTableInfo tableInfo;
    private IndexInfo indexInfo;
    private final List<TiExpr> fields = new ArrayList<>();
    private final List<TiExpr> where = new ArrayList<>();
    private final List<TiExpr> aggregates = new ArrayList<>();
    private int limit;
    private int timeZoneOffset;
    private long flags;
    private long startTs;
    private TiExpr having;
    private boolean distinct;

    public SelectRequest build() {
        List<TiColumnInfo> colToAdd = new ArrayList<>();
        this.fields.forEach(expr -> {
            this.builder.addFields(expr.toProto());
            colToAdd.addAll(getColumnInfoFromExpr(expr));
        });
        this.groupBys.forEach(expr -> this.builder.addGroupBy(expr.toProto()));
        this.orderBys.forEach(expr -> this.builder.addOrderBy(expr.toProto()));
        this.aggregates.forEach(expr -> {
            this.builder.addAggregates(expr.toProto());
            colToAdd.addAll(getColumnInfoFromExpr(expr));
        });
        this.builder.setFlags(flags);

        TableInfo table = TableInfo.newBuilder()
                .setTableId(tableInfo.getId())
                .addAllColumns(colToAdd.stream().map(TiColumnInfo::toProto).collect(Collectors.toList()))
                .build();
        this.builder.setTableInfo(table);
        this.builder.setTimeZoneOffset(timeZoneOffset);
        this.builder.setStartTs(startTs);
        return this.builder.build();
    }

    public static List<TiColumnInfo> getColumnInfoFromExpr(TiExpr expr) {
        List<TiColumnInfo> columnInfos = new ArrayList<>();
        if (expr instanceof TiFunctionExpression) {
            TiFunctionExpression tiF = (TiFunctionExpression)expr;
            tiF.getArgs().forEach(
                   arg -> {
                       if (arg instanceof TiColumnRef) {
                           TiColumnRef tiCR = (TiColumnRef) arg;
                           columnInfos.add(tiCR.getColumnInfo());
                       }
                   }
            );
        } else if (expr instanceof TiColumnRef) {
            columnInfos.add(((TiColumnRef)expr).getColumnInfo());
        }
        return columnInfos;
    }

        public static List<TiColumnRef> getColumnRefFromExpr(TiExpr expr) {
        List<TiColumnRef> columnRefss = new ArrayList<>();
        if (expr instanceof TiFunctionExpression) {
            TiFunctionExpression tiF = (TiFunctionExpression)expr;
            tiF.getArgs().forEach(
                   arg -> {
                       if (arg instanceof TiColumnRef) {
                           TiColumnRef tiCR = (TiColumnRef) arg;
                           columnRefss.add(tiCR);
                       }
                   }
            );
        } else if (expr instanceof TiColumnRef) {
            columnRefss.add(((TiColumnRef)expr));
        }
        return columnRefss;
    }

    public List<TiExpr> getFields() {
        return fields;
    }
    public List<TiByItem> getGroupBys() {
        return groupBys;
    }

    private final List<TiByItem> groupBys = new ArrayList<>();

    public List<TiByItem> getOrderBys() {
        return orderBys;
    }

    private final List<TiByItem> orderBys = new ArrayList<>();

    public List<TiExpr> getAggregates() {
        return aggregates;
    }

    public List<TiExpr> getWhere() {
        return where;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public void setTimeZoneOffset(int timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public void setHaving(TiExpr having) {
        this.having = having;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }

    public void setTableInfo(TiTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }
}
