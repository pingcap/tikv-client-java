package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.IndexScanIterator;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.Timer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {

  private static TiConfiguration conf =
      TiConfiguration.createDefault("127.0.0.1:" + 2379);
  private static TiSession session = TiSession.create(conf);
  private static Snapshot snapshot = session.createSnapshot();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("TPCH");
    TiTableInfo table = cat.getTable(db, "lineitem");
    TiIndexInfo index = table.getIndices().get(1);

    List<TiExpr> exprs =
        ImmutableList.of(
            new GreaterEqual(TiColumnRef.create("L_SHIPDATE", table),
                TiConstant.create("1993-07-30"))
            // new Equal(TiColumnRef.create("C_CUSTKEY", table), TiConstant.create(1111)),
            // new Equal(TiColumnRef.create("C_NATIONKEY", table), TiConstant.create(6)),
            //new NotEqual(TiColumnRef.create("c_address", table), TiConstant.create("test"))
        );

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, table);

    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .addRequiredColumn(TiColumnRef.create("L_ORDERKEY", table))
        .setStartTs(snapshot.getVersion());

    if (scanPlan.isIndexScan()) {
      selReq.setIndexInfo(scanPlan.getIndex());
    }
    System.out.println(scanPlan.getIndex().toString());
    System.out.println(selReq.toString());

    //selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));
    //List<RangeSplitter.RegionTask> keyWithRegionTasks =
    //    RangeSplitter.newSplitter(session.getRegionManager())
    //        .splitRangeByRegion(selReq.getRanges());

    IndexScanIterator.old = false;
    System.in.read();
    System.out.println("start");
    Timer t1 = new Timer();
    Iterator<Row> it = snapshot.tableRead(selReq);

    SchemaInfer schemaInfer = SchemaInfer.create(selReq);
    while (it.hasNext()) {
      Row r = it.next();
      /*for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
        System.out.print(" ");
      }
      System.out.print("\n");
      */
    }
    System.out.println("done t1:" + t1.stop(TimeUnit.SECONDS));
    session.close();
    System.exit(0);
  }
}
