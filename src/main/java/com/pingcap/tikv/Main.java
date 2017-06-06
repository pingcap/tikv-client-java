package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.First;
import com.pingcap.tikv.expression.aggregate.Sum;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.expression.TiExpr;
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
        TiExpr sum = new Sum(s1);
        TiExpr first = new First(s1);
        //select s1 from t1;
        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRange(TiRange.create(0L, Long.MAX_VALUE));
//        sb.addField(s1);
        sb.addAggregates(sum);
        sb.addAggregates(first);
//        start_ts:392368402957860865 table_info:<table_id:25 columns:<column_id:2 tp:15 collation:83 columnLen:11 decimal:-1
//        > > aggregates:<tp:Sum children:<tp:ColumnRef val:"\200\000\000\000\000\000\000\002"
//        > > aggregates:<tp:First children:<tp:ColumnRef val:"\200\000\000\000\000\000\000\002" > > time_zone_offset:28800 flags:1
        Iterator<Row> it = snapshot.select(table, sb.getTiSelectReq(), sb.getRangeListBuilder().build());
        while (it.hasNext()) {
            Row r = it.next();
            SchemaInferer.TiFieldType tf = SchemaInferer.create(sb.getTiSelectReq());
            for(int i = 0; i < r.fieldCount(); i++){
               Object val = r.get(i, tf.getFieldType(i));
               System.out.println(val);
            }
        }

        cluster.close();
    }
}
