package com.pingcap.tikv;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.IndexInfo;
import com.pingcap.tidb.tipb.Select;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.operation.SchemaInferer;
import lombok.Data;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tidb.tipb.ExprType;

import java.util.*;

@Data
public class SelectBuilder {
  private static long MASK_IGNORE_TRUNCATE = 0x1;
  private static long MASK_TRUNC_AS_WARNING = 0x2;

  private final Snapshot snapshot;
  private final ImmutableList.Builder<TiRange<Long>> rangeListBuilder;
  private TiTableInfo table;
  private TiSelectRequest tiSelectReq;
  private long timestamp;
  private long timeZoneOffset;
  private boolean distinct;


  private TiSession getSession() {
    return snapshot.getSession();
  }

  private TiConfiguration getConf() {
    return getSession().getConf();
  }

  public static SelectBuilder newBuilder(Snapshot snapshot, TiTableInfo table) {
    return new SelectBuilder(snapshot, table);
  }

  private SelectBuilder(Snapshot snapshot, TiTableInfo table) {
    this.snapshot = snapshot;
    this.rangeListBuilder = ImmutableList.builder();
    this.table = table;
    this.tiSelectReq = new TiSelectRequest(SelectRequest.newBuilder());

    long flags = 0;
    if (getConf().isIgnoreTruncate()) {
      flags |= MASK_IGNORE_TRUNCATE;
    } else if (getConf().isTruncateAsWarning()) {
      flags |= MASK_TRUNC_AS_WARNING;
    }
    tiSelectReq.setFlags(flags);
    tiSelectReq.setStartTs(snapshot.getVersion());
    // Set default timezone offset
    TimeZone tz = TimeZone.getDefault();
    tiSelectReq.setTimeZoneOffset(tz.getOffset(new Date().getTime()) / 1000);
    tiSelectReq.setTableInfo(table);
  }

  public SelectBuilder setTimeZoneOffset(long offset) {
//    builder.setTimeZoneOffset(offset);
    return this;
  }

  public SelectBuilder setTimestamp(long timestamp) {
//    builder.setStartTs(timestamp);
    return this;
  }

  private boolean isExprTypeSupported(ExprType exprType) {
    switch (exprType) {
      case Null:
      case Int64:
      case Uint64:
      case String:
      case Bytes:
      case MysqlDuration:
      case MysqlTime:
      case MysqlDecimal:
      case ColumnRef:
      case And:
      case Or:
      case LT:
      case LE:
      case EQ:
      case NE:
      case GE:
      case GT:
      case NullEQ:
      case In:
      case ValueList:
      case Like:
      case Not:
        return true;
      case Plus:
      case Div:
        return true;
      case Case:
      case If:
        return true;
      case Count:
      case First:
      case Max:
      case Min:
      case Sum:
      case Avg:
        return true;
        // TODO: finish this
        // case kv.ReqSubTypeDesc:
        // return true;
      default:
        return false;
    }
  }

  // projection
  public SelectBuilder fields(TiExpr expr) {
      this.tiSelectReq.getFields().add(expr);
      return this;
  }

  public SelectBuilder addRange(TiRange<Long> keyRange) {
    rangeListBuilder.add(keyRange);
    return this;
  }

  public SelectBuilder distinct(boolean distinct) {
      this.tiSelectReq.setDistinct(distinct);
      return this;
  }

  public SelectBuilder where(TiExpr expr) {
      if(this.tiSelectReq.getWhere() == null) {
          this.tiSelectReq.setWhere( new ArrayList<>());
      }
    this.tiSelectReq.getWhere().add(expr);
    return this;
  }

  public SelectBuilder groupBy(TiByItem value) {
      if(this.tiSelectReq.getGroupBys() == null) {
          this.tiSelectReq.setGroupBys(new ArrayList<>());
      }
      this.tiSelectReq.getGroupBys().add(value);
      return this;
  }

  public SelectBuilder addHaving(TiExpr expr) {
      this.tiSelectReq.setHaving(expr);
      return this;
  }

  public SelectBuilder orderBy(TiByItem value) {
      if(this.tiSelectReq.getOrderBys() == null) {
          this.tiSelectReq.setOrderBys(new ArrayList<>());
      }
    this.tiSelectReq.getOrderBys().add(value);
    return this;
  }

  public SelectBuilder limit(int limit) {
      this.tiSelectReq.setLimit(limit);
      return this;
  }

  public SelectBuilder addAggregates(TiExpr expr) {
      if (this.tiSelectReq.getAggregates() == null) {
          this.tiSelectReq.setAggregates(new ArrayList<>());
      }
      this.tiSelectReq.getAggregates().add(expr);
      return this;
  }

  public SelectBuilder addField(TiExpr expr) {
      this.tiSelectReq.getFields().add(expr);
      return this;
  }



  public Iterator<Row> doSelect() {
    checkNotNull(table);
    List<TiRange<Long>> ranges = rangeListBuilder.build();
    checkArgument(ranges.size() > 0);
    return snapshot.select(table, this.tiSelectReq, ranges);
  }
}
