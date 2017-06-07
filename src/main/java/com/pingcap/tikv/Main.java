package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.meta.TiExpr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.meta.TiTableInfo;

import java.util.Iterator;

public class Main {
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    TiCluster cluster = TiCluster.getCluster(conf);
    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "booleantable");
    Snapshot snapshot = cluster.createSnapshot();
    Iterator<Row> it =
        snapshot
            .newSelect(table)
            .addRange(TiRange.create(0L, Long.MAX_VALUE))
            .doSelect();

    while (it.hasNext()) {
      Row r = it.next();
      int val0 = r.getInteger(0);
      System.out.print("");
    }

    cluster.close();
    return;
  }
}
