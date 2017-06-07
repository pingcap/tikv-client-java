package com.pingcap.tikv.meta;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tidb.tipb.IndexInfo;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.util.TiFluentIterable;
import jdk.nashorn.internal.ir.annotations.Immutable;
import lombok.Data;

import java.util.*;

@Data
  public class TiSelectRequest {
      private final SelectRequest.Builder builder;
      private TiTableInfo tableInfo;
      private IndexInfo indexInfo;
      private List<TiExpr> fields = new ArrayList<>();
      private List<TiExpr> where = new ArrayList<>();
      private List<TiByItem> groupBys = new ArrayList<>();
      private List<TiByItem> orderBys = new ArrayList<>();
      private List<TiExpr> aggregates = new ArrayList<>();
      private int limit;
      private int timeZoneOffset;
      private long flags;
      private long startTs;
      private TiExpr having;
      private boolean distinct;

      public SelectRequest toProto() {
        return null;
      }

      public SelectRequest build() {
        // add optimize later
        // Optimize merge groupBy
        this.aggregates.forEach(expr -> this.builder.addAggregates(expr.toProto()));
        this.fields.forEach(expr -> this.builder.addFields(expr.toProto()));
        if(!this.fields.isEmpty()) {
            // convert fields type to set for later usage.
            Set<String> fieldSet = ImmutableSet.copyOf(TiFluentIterable.from(this.fields).transform(
                    expr -> ((TiColumnRef)expr).getName()
            ));
            // solve
            List<TiColumnInfo> colToRemove = new ArrayList<>();
            this.tableInfo.getColumns().forEach(
                            col -> {
                                // remove column from table if such column is not in fields
                                if(!fieldSet.contains(col.getName())) {
                                    colToRemove.add(col);
                                }
                            }
            );

            this.tableInfo.getColumns().removeAll(colToRemove);
        }
        this.groupBys.forEach(expr -> this.builder.addGroupBy(expr.toProto()));
        this.orderBys.forEach(expr -> this.builder.addOrderBy(expr.toProto()));
        this.aggregates.forEach(expr -> this.builder.addAggregates(expr.toProto()));
        this.builder.setFlags(flags);
        this.builder.setTableInfo(tableInfo.toProto());
        this.builder.setTimeZoneOffset(timeZoneOffset);
        this.builder.setStartTs(startTs);
        return this.builder.build();
        }
  }
