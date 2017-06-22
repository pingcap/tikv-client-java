package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
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
    private static List<KeyRange> getFullRange(TiTableInfo table) {
        ByteString startKey = TableCodec.encodeRowKeyWithHandle(table.getId(), 1L);
        ByteString endKey = TableCodec.encodeRowKeyWithHandle(table.getId(), 2);
        return ImmutableList.of(KeyRange.newBuilder().setLow(startKey).setHigh(endKey).build());
    }

    public static void main(String[] args) throws Exception {
        // May need to save this reference
        Logger log = Logger.getLogger("io.grpc");
        log.setLevel(Level.WARNING);

        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "test");
        Snapshot snapshot = cluster.createSnapshot();
        TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);

        List<TiExpr> exprs = ImmutableList.of(
                new Equal(TiColumnRef.create("c1", table),
                          TiConstant.create(2L))
        );
        ScanBuilder scanBuilder = new ScanBuilder();
        ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(exprs, index, table);

        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRanges(scanPlan.getKeyRanges());

        sb.addField(TiColumnRef.create("c1", table));
        sb.addField(TiColumnRef.create("c2", table));
        sb.addField(TiColumnRef.create("c3", table));
        sb.addField(TiColumnRef.create("c4", table));

        Iterator<Row> it = snapshot.select(sb);

        while (it.hasNext()) {
            Row r = it.next();
            SchemaInfer schemaInfer = SchemaInfer.create(sb.getTiSelectReq());
//            RowTransformer.Builder builder = RowTransformer.newBuilder();
//            builder.addProjection(new Skip(null));
//            builder.addProjection(new Cast(DataTypeFactory.of(Types.TYPE_LONG)));
//            builder.addSourceFieldTypes(schemaInfer.getTypes());
//            r = builder.build().transform(r);
            for (int i = 0; i < r.fieldCount(); i++) {
                Object val = r.get(i, schemaInfer.getType(i));
                System.out.print(val);
                System.out.print(" ");
            }
            System.out.print("\n");
        }
    }
}
