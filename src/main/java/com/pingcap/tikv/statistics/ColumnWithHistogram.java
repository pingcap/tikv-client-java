package com.pingcap.tikv.statistics;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiKey;
import com.pingcap.tikv.predicates.RangeBuilder.IndexRange;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.List;
import java.util.Set;

import static com.pingcap.tikv.types.Types.TYPE_LONG;

/**
 * Created by birdstorm on 2017/8/14.
 * may be deleted according to TiDB's implementation
 */
public class ColumnWithHistogram {
  private Histogram hg;
  private TiColumnInfo info;

  public ColumnWithHistogram(Histogram hist, TiColumnInfo colInfo) {
    this.hg = hist;
    this.info = colInfo;
  }

  long getLastUpdateVersion() {
    return hg.getLastUpdateVersion();
  }

  /** getColumnRowCount estimates the row count by a slice of ColumnRange. */
  double getColumnRowCount(List<IndexRange> columnRanges) {
    double rowCount = 0.0;
    for (IndexRange range : columnRanges) {
      double cnt = 0.0;
      List<Object> points = range.getAccessPoints();
      if (!points.isEmpty()) {
        if (points.size() > 1) {
          System.out.println("Warning: ColumnRowCount should only contain one attribute.");
        }
        cnt = hg.equalRowCount(TiKey.create(points.get(0)));
        assert range.getRange() == null;
      } else if (range.getRange() != null){
        Range rg = range.getRange();
        TiKey lowerBound, upperBound;
        DataType t;
        boolean lNull = !rg.hasLowerBound();
        boolean rNull = !rg.hasUpperBound();
        boolean lOpen = lNull || rg.lowerBoundType().equals(BoundType.OPEN);
        boolean rOpen = rNull || rg.upperBoundType().equals(BoundType.OPEN);
        String l = lOpen ? "(" : "[";
        String r = rOpen ? ")" : "]";


        CodecDataOutput cdo = new CodecDataOutput();
        Object lower = TiKey.unwrap(!lNull ? rg.lowerEndpoint() : DataType.indexMinValue());
        Object upper = TiKey.unwrap(!rNull ? rg.upperEndpoint() : DataType.indexMaxValue());

//        System.out.println("=>" + l + (!lNull ? lower : "-∞") + "," + (!rNull ? upper : "∞") + r);

        t = DataTypeFactory.of(TYPE_LONG);
        if(lNull) {
          t.encodeMinValue(cdo);
        } else {
          t.encode(cdo, DataType.EncodeType.KEY, lower);
          if(lOpen) {
            cdo.writeByte(0);
          }
        }
        lowerBound = TiKey.create(cdo.toByteString());

        cdo.reset();
        if(rNull) {
          t.encodeMaxValue(cdo);
        } else {
          t.encode(cdo, DataType.EncodeType.KEY, upper);
          if(!rOpen) {
            cdo.writeByte(0);
          }
        }
        upperBound = TiKey.create(cdo.toByteString());

        System.out.print(l + lowerBound + "," + upperBound + r);
        cnt += hg.betweenRowCount(lowerBound, upperBound);
      }
      rowCount += cnt;
    }
    if (rowCount > hg.totalRowCount()) {
      rowCount = hg.totalRowCount();
    } else if (rowCount < 0) {
      rowCount = 0;
    }
    return rowCount;
  }

  public Histogram getHistogram() {
    return hg;
  }

  public TiColumnInfo getColumnInfo() {
    return info;
  }

  public static TiColumnRef indexInfo2Col(Set<TiColumnRef> cols, TiColumnInfo col) {
    return null;
  }

}
