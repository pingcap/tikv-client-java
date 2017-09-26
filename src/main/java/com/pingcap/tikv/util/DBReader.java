package com.pingcap.tikv.util;

import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by birdstorm on 2017/9/6.
 *
 */
public class DBReader {
  private Catalog cat;
  private Snapshot snapshot;
  private RegionManager manager;
  private TiConfiguration conf;
  private TiDBInfo db;

  public DBReader() {}

  public DBReader(Catalog cat, String DBName, Snapshot snapshot, RegionManager manager, TiConfiguration conf) {
    this.cat = cat;
    setCurrentDB(DBName);
    this.snapshot = snapshot;
    this.manager = manager;
    this.conf = conf;
  }

  private void setCurrentDB(String DBName) {
    this.db = cat.getDatabase(DBName);
  }

  public Catalog getCatalog() {
    return cat;
  }

  public void setCatalog(Catalog cat) {
    this.cat = cat;
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(Snapshot snapshot) {
    this.snapshot = snapshot;
  }

  public RegionManager getRegionManager() {
    return manager;
  }

  public void setRegionManager(RegionManager manager) {
    this.manager = manager;
  }

  public TiTableInfo getTableInfo(String tableName) {
    return cat.getTable(db, tableName);
  }

  public TiTableInfo getTableInfo(long tableID) {
    return cat.getTable(db, tableID);
  }

  private TiSelectRequest getSelectRequest(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    TiTableInfo tableInfo = getTableInfo(tableName);
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(tableInfo);

    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, index, tableInfo);
    TiSelectRequest selReq = new TiSelectRequest();

    //build select request
    selReq.addRanges(scanPlan.getKeyRanges()).setTableInfo(tableInfo);
    //add fields
    for(String s: returnFields) {
      selReq.addField(TiColumnRef.create(s, tableInfo));
    }
    selReq.setStartTs(snapshot.getVersion());

    if (conf.isIgnoreTruncate()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.IgnoreTruncation);
    } else if (conf.isTruncateAsWarning()) {
      selReq.setTruncateMode(TiSelectRequest.TruncateMode.TruncationAsWarning);
    }

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    return selReq;
  }

  private List<Row> getSelectedRows(TiSelectRequest selReq) {
    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(manager).
            splitRangeByRegion(selReq.getRanges());

    List<Row> rowList = new ArrayList<>();

    for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, task);
      while (it.hasNext()) {
        Row row = it.next();
        rowList.add(row);
      }
    }
    return rowList;
  }

  public List<Row> getSelectedRows(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    return getSelectedRows(getSelectRequest(tableName, exprs, returnFields));
  }

  private void printRows(List<Row> rows, TiSelectRequest selReq) {
    for(Row r: rows) {
      SchemaInfer schemaInfer = SchemaInfer.create(selReq);
      for (int i = 0; i < r.fieldCount(); i++) {
        Object c = r.get(i, schemaInfer.getType(i));
        System.out.print(TiKey.create(c));
        System.out.print(" ");
      }
      System.out.print("\n");
    }
  }

  public void printRows(String tableName, List<TiExpr> exprs, List<String> returnFields) {
    TiSelectRequest selectRequest = getSelectRequest(tableName, exprs, returnFields);
    List<Row> rows = getSelectedRows(selectRequest);
    printRows(rows, selectRequest);
  }


}
