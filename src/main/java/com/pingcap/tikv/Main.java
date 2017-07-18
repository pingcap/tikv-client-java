package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.IsNull;
import com.pingcap.tikv.expression.scalar.Not;
import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

  private static TiConfiguration conf =
      TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
  private static TiCluster cluster = TiCluster.getCluster(conf);
  private static Snapshot snapshot = cluster.createSnapshot();

  public static void main(String[] args) throws Exception {
    // May need to save this reference
    Logger log = Logger.getLogger("io.grpc");
    log.setLevel(Level.WARNING);
    PDClient client = PDClient.createRaw(cluster.getSession());
    for (int i = 0; i < 51; i++) {
      TiRegion r = client.getRegionByID(i);
      r.getId();
    }

    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase("tpch");
    TiTableInfo table = cat.getTable(db, "lineitem");

    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);

    List<TiExpr> exprs =
        ImmutableList.of(
            new Not(new IsNull(TiColumnRef.create("dept", table))),
            new Equal(TiColumnRef.create("dept", table), TiConstant.create("computer")));

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, index, table);

    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .setIndexInfo(index)
        .addRequiredColumn(TiColumnRef.create("id", table))
        .addRequiredColumn(TiColumnRef.create("name", table))
        .addRequiredColumn(TiColumnRef.create("quantity", table))
        .addRequiredColumn(TiColumnRef.create("dept", table))
        .setStartTs(snapshot.getVersion());

    if (conf.isIgnoreTruncate()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.IgnoreTruncation);
    } else if (conf.isTruncateAsWarning()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.TruncationAsWarning);
    }

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    System.out.println(exprs);
    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(cluster.getRegionManager())
            .splitRangeByRegion(selReq.getRanges());
    for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, task);

      while (it.hasNext()) {
        Row r = it.next();
        SchemaInfer schemaInfer = SchemaInfer.create(selReq);
        for (int i = 0; i < r.fieldCount(); i++) {
          Object val = r.get(i, schemaInfer.getType(i));
          //printByHandle(table, (long)val, scanPlan.getFilters());
          System.out.print(val);
          System.out.print(" ");
        }
        System.out.print("\n");
      }
    }
  }

  private static void printByHandle(TiTableInfo table, long handle, List<TiExpr> filters) {
    ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), handle);
    ByteString endKey = ByteString.copyFrom(KeyUtils.prefixNext(startKey.toByteArray()));

    TiSelectRequest selReq = new TiSelectRequest();
    selReq.addRanges(
        ImmutableList.of(
            Coprocessor.KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build()));
    selReq.addRequiredColumn(TiColumnRef.create("c1", table));
    selReq.addRequiredColumn(TiColumnRef.create("c2", table));
    selReq.addRequiredColumn(TiColumnRef.create("c3", table));
    selReq.addRequiredColumn(TiColumnRef.create("c4", table));
    if (filters != null) {
      filters.stream().forEach(selReq::addWhere);
    }

    Iterator<Row> it = snapshot.select(selReq);

    while (it.hasNext()) {
      Row r = it.next();
      SchemaInfer schemaInfer = SchemaInfer.create(selReq);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object val = r.get(i, schemaInfer.getType(i));
        System.out.print(val);
        System.out.print(" ");
      }
      System.out.print("\n");
    }
  }
}
