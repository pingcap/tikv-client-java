package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Count;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import org.apache.log4j.Logger;

public class Main {
  private static final Logger logger = Logger.getLogger(Main.class);
  public static void main(String[] args) throws Exception {
    TiConfiguration conf = TiConfiguration.createDefault("127.0.0.1:2379");
    TiSession session = TiSession.create(conf);
    Catalog cat = session.getCatalog();
    TiDBInfo db = cat.getDatabase("test");
    TiTableInfo table = cat.getTable(db, "t1");
    Snapshot snapshot = session.createSnapshot();
    TiSelectRequest selectRequest = new TiSelectRequest();
    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(new ArrayList<>(), table);
    selectRequest.addRequiredColumn(TiColumnRef.create("c1", table))
        .addRequiredColumn(TiColumnRef.create("c2", table))
        .setStartTs(snapshot.getVersion())
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table);
    Iterator<Row> it = snapshot.tableRead(selectRequest);
    while (it.hasNext()) {
      Row r = it.next();
      SchemaInfer schemaInfer = SchemaInfer.create(selectRequest);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
      }
    }
    session.close();
  }

  private static void tableScan(Catalog cat, TiSession session, String dbName, String tableName) throws Exception {
    TiDBInfo db = cat.getDatabase(dbName);
    TiTableInfo table = cat.getTable(db, tableName);
    Snapshot snapshot = session.createSnapshot();

    logger.info(String.format("Table Scan Start: db[%s] table[%s]", dbName, tableName));

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildTableScan(ImmutableList.of(), table);

    TiSelectRequest selReq = new TiSelectRequest();
    TiColumnRef firstColumn = TiColumnRef.create(table.getColumns().get(0).getName(), table);
    selReq.setTableInfo(table)
          .addAggregate(new Count(firstColumn))
          .addRequiredColumn(firstColumn)
          .setStartTs(snapshot.getVersion());

    List<RegionTask> regionTasks = RangeSplitter
        .newSplitter(session.getRegionManager())
        .splitRangeByRegion(scanPlan.getKeyRanges());
    for (RegionTask task : regionTasks) {
      Iterator<Row> it = snapshot.tableRead(selReq, ImmutableList.of(task));
      Row row = it.next();
    }
  }
}
