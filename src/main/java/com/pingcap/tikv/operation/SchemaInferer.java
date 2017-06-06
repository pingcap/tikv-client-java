
package com.pingcap.tikv.operation;


import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.type.LongType;
import com.pingcap.tikv.util.TiFluentIterable;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.type.StringType;
import com.pingcap.tikv.type.DecimalType;
import lombok.Data;

import javax.swing.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaInferer {
    // type match
        // sum -> decimal
        // max -> columnRef
        // min -> columnRef
        // count -> LongType
        //
    // indices changes
    public static FieldType[] toFieldTypes(SelectRequest req) {
        // select c1, s1 from t1
        if (req.getAggregatesCount() == 0) {
            return TiFluentIterable.from(req.getTableInfo().getColumnsList())
                .transform(column -> new TiColumnInfo.InternalTypeHolder(column).toFieldType())
                .toArray(FieldType.class);
        } else {
            // TODO: add more aggregates type
            FieldType[] fts = new FieldType[2];
            fts[0] = new StringType();
            fts[1] = DecimalType.DEF_DECIMAL;
            return fts;
        }
    }

    // maybe a iterator
    @Data
    public static class TiFieldType {
        List<FieldType> fieldTypes;
        public static final Map<Integer, FieldType> mysqlTpToFieldType = ImmutableMap.of(
                DecimalType.DEF_DECIMAL.getTypeCode(), DecimalType.DEF_DECIMAL,
                LongType.DEF_VAR_LONG.getTypeCode(), LongType.DEF_VAR_LONG,
                StringType.DEF_STRING.getTypeCode(), StringType.DEF_STRING
        );
    }

    public static TiFieldType toFieldTypes(TiSelectRequest tiSelectRequest) {
        // type match
        // sum -> decimal
        // max -> columnRef
        // min -> columnRef
        // count -> LongType
        //
        // indices changes
        // no aggregates function in select
        TiFieldType tf = new TiFieldType();
        List<FieldType> fs = new ArrayList<>();
        if (tiSelectRequest.getAggregates().isEmpty()) {
            tiSelectRequest.getFields().forEach(
                    expr -> {
                        // if expr is column reference
                        // extract columninfo
                        fs.add(expr.getType());
                    }
            );
        }
        tf.setFieldTypes(fs);
        return tf;
    }
}
