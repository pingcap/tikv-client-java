package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.expression.scalar.NotEqual;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Main {

    private static TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    private static TiCluster cluster = TiCluster.getCluster(conf);
    private static Snapshot snapshot = cluster.createSnapshot();

    public static void main(String[] args) throws Exception {
        // May need to save this reference
        Logger log = Logger.getLogger("io.grpc");
        log.setLevel(Level.WARNING);

        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "test");

        TiIndexInfo index = table.getIndices().get(0);

        List<TiExpr> exprs = ImmutableList.of(
                new NotEqual(TiColumnRef.create("c1", table),
                             TiConstant.create(4L)),
                new GreaterThan(TiColumnRef.create("c4", table),
                        TiConstant.create(100L))
        );

        ScanBuilder scanBuilder = new ScanBuilder();
        ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, index, table);

        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRanges(scanPlan.getKeyRanges());
        sb.setIndex(index);

        sb.addField(TiColumnRef.create("c1", table));
        //sb.addField(TiColumnRef.create("c2", table));
        //sb.addField(TiColumnRef.create("c3", table));
        //sb.addField(TiColumnRef.create("c4", table));
        // scanPlan.getFilters().stream().forEach(sb::addWhere);

        System.out.println(exprs);
        Iterator<Row> it = snapshot.select(sb);

        while (it.hasNext()) {
            Row r = it.next();
            SchemaInfer schemaInfer = SchemaInfer.create(sb.getTiSelectReq());
            for (int i = 0; i < r.fieldCount(); i++) {
                Object val = r.get(i, schemaInfer.getType(i));
                printByHandle(table, (long)val, scanPlan.getFilters());
                //System.out.print(val);
                //System.out.print(" ");
            }
            System.out.print("\n");
        }
    }

    private static void printByHandle(TiTableInfo table, long handle, List<TiExpr> filters) {
        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), handle);
        ByteString endKey = ByteString.copyFrom(KeyUtils.prefixNext(startKey.toByteArray()));

        sb.addRanges(ImmutableList.of(KeyRange.newBuilder().setLow(startKey).setHigh(endKey).build()));
        sb.addField(TiColumnRef.create("c1", table));
        sb.addField(TiColumnRef.create("c2", table));
        sb.addField(TiColumnRef.create("c3", table));
        sb.addField(TiColumnRef.create("c4", table));
        if (filters != null) {
            filters.stream().forEach(sb::addWhere);
        }

        Iterator<Row> it = snapshot.select(sb);

        while (it.hasNext()) {
            Row r = it.next();
            SchemaInfer schemaInfer = SchemaInfer.create(sb.getTiSelectReq());
            for (int i = 0; i < r.fieldCount(); i++) {
                Object val = r.get(i, schemaInfer.getType(i));
                System.out.print(val);
                System.out.print(" ");
            }
            System.out.print("\n");
        }
    }
}
