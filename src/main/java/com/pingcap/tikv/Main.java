package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.SchemaInferer;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "t1");
        Snapshot snapshot = cluster.createSnapshot();

        TiExpr s1 = TiColumnRef.create("s1", table);
        //select s1 from t1;
        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRange(TiRange.create(0L, Long.MAX_VALUE));
        sb.addField(s1);
        Iterator<Row> it = snapshot.select(table, sb.getTiSelectReq(), sb.getRangeListBuilder().build());
        while (it.hasNext()) {
            Row r = it.next();
            SchemaInferer.TiFieldType tf = SchemaInferer.toFieldTypes(sb.getTiSelectReq());
            for(int i = 0; i < r.fieldCount(); i++){
               Object val = r.get(i, tf.getFieldTypes().get(i));
               System.out.println(val);
            }
        }

        cluster.close();
    }
}
