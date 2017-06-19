package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.aggregate.Sum;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.row.Row;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) throws Exception {
        // May need to save this reference
        Logger log = Logger.getLogger("io.grpc");
        log.setLevel(Level.WARNING);
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
        TiCluster cluster = TiCluster.getCluster(conf);
        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("test");
        TiTableInfo table = cat.getTable(db, "t1");
        Snapshot snapshot = cluster.createSnapshot();

        TiExpr number = TiColumnRef.create("number", table);
        TiExpr name = TiColumnRef.create("name", table);
        TiExpr sum = new Sum(number);
        TiByItem groupBy = TiByItem.create(name, false);
        // TiExpr first = new First(s1);
        SelectBuilder sb = SelectBuilder.newBuilder(snapshot, table);
        sb.addRange(TiRange.create(0L, Long.MAX_VALUE));
        //select name, sum(number) from t1 group by name;
        sb.addAggregate(sum);
        sb.addField(name);
        sb.addGroupBy(groupBy);

        Iterator<Row> it = snapshot.select(sb);

        while (it.hasNext()) {
            Row r = it.next();
            SchemaInfer schemaInfer  = SchemaInfer.create(sb.getTiSelectReq());
//            RowTransformer.Builder builder = RowTransformer.newBuilder();
//            builder.addProjection(new Skip(null));
//            builder.addProjection(new Cast(DataTypeFactory.of(Types.TYPE_LONG)));
//            builder.addSourceFieldTypes(schemaInfer.getTypes());
//            r = builder.build().transform(r);
            for(int i = 0; i < r.fieldCount(); i++){
               Object val = r.get(i, schemaInfer.getType(i));
               System.out.println(val);
            }
        }
        cluster.close();
    }
}
