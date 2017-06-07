/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.FieldType;

public class TiColumnRef implements TiExpr {
    static public TiColumnInfo getColumnWithName(String name, TiTableInfo table) {
        TiColumnInfo columnInfo = null;
        for (TiColumnInfo col : table.getColumns()) {
            if (col.matchName(name)) {
                columnInfo = col;
                break;
            }
        }
        return columnInfo;
    }
    static public TiColumnRef create(String name, TiTableInfo table) {
        TiColumnInfo columnInfo = getColumnWithName(name, table);
        if (columnInfo == null) {
            throw new TiExpressionException("No Matching columns from TiTableInfo");
        }

        // TODO: After type system finished, do a type check
        //switch column.GetType().Tp {
        //    case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeUnspecified:
        //        return nil
        //}

        if (columnInfo.getId() == 0) {
            throw new TiExpressionException("Zero Id is not a referable column id");
        }
        return new TiColumnRef(columnInfo);
    }

    private TiColumnInfo columnInfo;

    private TiColumnRef(TiColumnInfo columnInfo) {
        this.columnInfo = columnInfo;
    }

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        builder.setTp(ExprType.ColumnRef);
        CodecDataOutput cdo = new CodecDataOutput();
        LongUtils.writeLong(cdo, columnInfo.getId());
        builder.setVal(cdo.toByteString());
        return builder.build();
    }

    @Override
    public FieldType getType() {
        return columnInfo.getType();
    }

    public String getName() {
        return this.columnInfo.getName();
    }
}
