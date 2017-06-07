package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Sum;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.operation.transformer.Cast;
import com.pingcap.tikv.operation.transformer.NoOp;
import com.pingcap.tikv.operation.transformer.RowTransformer;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.operation.transformer.Skip;
import com.pingcap.tikv.types.integer.LongType;

import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws Exception {
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "t1");
        Snapshot snapshot = cluster.createSnapshot();

//        TiExpr s1 = TiColumnRef.create("s1", table);
        TiExpr c1 = TiColumnRef.create("c1", table);
        TiExpr sum = new Sum(c1);
        TiByItem groupBy = TiByItem.create(c1, false);
        // TiExpr first = new First(s1);
        //select s1 from t1;
        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRange(TiRange.create(0L, Long.MAX_VALUE));
        sb.addAggregate(sum);
        sb.addGroupBy(groupBy);

        Iterator<Row> it = snapshot.select(sb);

        while (it.hasNext()) {
            Row r = it.next();
            SchemaInfer schemaInfer  = SchemaInfer.create(sb.getTiSelectReq());
            RowTransformer.Builder builder = RowTransformer.newBuilder();
            builder.addProjection(new Skip(null));
            builder.addProjection(new Cast(LongType.DEF_SIGNED_TYPE));
            builder.addSourceFieldTypes(schemaInfer.getFieldTypes());
            r = builder.build().transform(r);
            for(int i = 0; i < r.fieldCount(); i++){
               Object val = r.get(i, schemaInfer.getFieldType(i));
               System.out.println(val);
            }
        }
        cluster.close();
    }
}
