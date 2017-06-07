package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.meta.TiSelectRequest;
import lombok.Data;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tidb.tipb.ExprType;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * SelectBuilder is builder that you can build a select request which can be sent to TiKV and PD.
 */
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

  /**
   * add timezone offset to select request.
   * @param offset is time zone offset.
   * @return a SelectBuilder
   */
  public SelectBuilder setTimeZoneOffset(int offset) {
     this.tiSelectReq.setTimeZoneOffset(offset);;
     return this;
  }

  /**
   * add timestamp to select request.
   * @param timestamp is a timestamp
   * @return a SelectBuilder
   */
  public SelectBuilder setTimestamp(long timestamp) {
    this.tiSelectReq.setStartTs(timestamp);
    return this;
  }

  /**
   * add key range in select request. This will be used in extract data from TiKV and PD.
   * @param keyRange
   * @return
   */
  public SelectBuilder addRange(TiRange<Long> keyRange) {
      rangeListBuilder.add(keyRange);
      return this;
  }

  /**
   * If distinct is set to true, then query result should be distinct
   * @param distinct value for distinct
   * @return return a SelectBuilder
   */
  public SelectBuilder distinct(boolean distinct) {
      this.tiSelectReq.setDistinct(distinct);
      return this;
  }

  /**
   * add a where condition to select query.
   * @param expr is a expression
   * @return a SelectBuilder
   */
  public SelectBuilder addWhere(TiExpr expr) {
    this.tiSelectReq.getWhere().add(expr);
    return this;
  }

  /**
   * add a group by clause to select query
   * @param byItem is a TiByItem
   * @return a SelectBuilder
   */
  public SelectBuilder addGroupBy(TiByItem byItem) {
      this.tiSelectReq.getGroupBys().add(byItem);
      return this;
  }

  /**
   * add a having clause to select query
   * @param expr is a expression represents Having
   * @return a SelectBuilder
   */
  public SelectBuilder addHaving(TiExpr expr) {
      this.tiSelectReq.setHaving(expr);
      return this;
  }

  /**
   * add a order by clause to select query.
   * @param byItem is a TiByItem.
   * @return a SelectBuilder
   */
  public SelectBuilder addOrderBy(TiByItem byItem) {
    this.tiSelectReq.getOrderBys().add(byItem);
    return this;
  }

  /**
   * add limit clause to select query.
   * @param limit is just a integer.
   * @return a SelectBuilder
   */
  public SelectBuilder limit(int limit) {
      this.tiSelectReq.setLimit(limit);
      return this;
  }

  /**
   * add aggregate function to select query
   * @param expr is a TiUnaryFunction expression.
   * @return a SelectBuilder
   */
  public SelectBuilder addAggregate(TiExpr expr) {
      this.tiSelectReq.getAggregates().add(expr);
      return this;
  }

  /*
  * field is not support in TiDB yet, but we still implement this
  * in case of TiDB support it in future.
  * The usage of this function will be simple query such as select c1 from t.
  * @param expr expr is a TiExpr. It is usually TiColumnRef.
   */
  public SelectBuilder addField(TiExpr expr) {
      this.tiSelectReq.getFields().add(expr);
      return this;
  }

  public List<TiRange<ByteString>> convertHandleRangeToKeyRange() {
        ImmutableList.Builder<TiRange<ByteString>> builder = ImmutableList.builder();
        for (TiRange<Long> r : getRangeListBuilder().build()) {
            ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), r.getLowValue());
            ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(),
                    Math.max(r.getHighValue() + 1, Long.MAX_VALUE));
            builder.add(TiRange.createByteStringRange(startKey, endKey));
        }
        return builder.build();
    }
}
