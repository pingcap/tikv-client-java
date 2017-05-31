package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiAggregateFunction;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiScalarExpression;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.meta.TiTableInfo;

import java.util.Iterator;

public class Main {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "t1");
    Snapshot snapshot = cluster.createSnapshot();
    Iterator<Row> it = null;

    TiAggregateFunction.create(TiAggregateFunction.AggFunc.Average,
            TiScalarExpression.create(TiScalarExpression.Op.Xor, TiConstant.create(1), TiConstant.create(2))
            ).toProto();

    while (it.hasNext()) {
      Row r = it.next();
      String val2 = r.getString(0);
      double val1 = r.getDecimal(1);
      // String val3 = r.getString(1);
      System.out.println(val2);
      System.out.println(val1);
      // System.out.println(val3);
    }

    cluster.close();
    return;
  }
}
