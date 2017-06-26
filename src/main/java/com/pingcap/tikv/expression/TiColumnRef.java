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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

public class TiColumnRef implements TiExpr {
     public static TiColumnInfo getColumnWithName(String name, TiTableInfo table) {
        TiColumnInfo columnInfo = null;
        for (TiColumnInfo col : table.getColumns()) {
            if (col.matchName(name)) {
                columnInfo = col;
                break;
            }
        }
        return columnInfo;
    }

    public static TiColumnRef create(String name) {
         return new TiColumnRef(name);
    }

    public static TiColumnRef create(String name, TiTableInfo table) {
        TiColumnRef ref = new TiColumnRef(name);
        ref.bind(table);
        return ref;
    }

    private final String name;

    private TiColumnInfo columnInfo;
    private TiTableInfo tableInfo;

    private TiColumnRef(String name) {
        this.name = name;
    }

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        builder.setTp(ExprType.ColumnRef);
        CodecDataOutput cdo = new CodecDataOutput();
        IntegerType.writeLong(cdo, columnInfo.getId());
        builder.setVal(cdo.toByteString());
        return builder.build();
    }

    @Override
    public DataType getType() {
        return columnInfo.getType();
    }

    @Override
    public void bind(TiTableInfo table) {
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
        this.tableInfo = table;
        this.columnInfo = columnInfo;
    }

    public String getName() {
        return name;
    }

    public TiColumnInfo getColumnInfo() {
        if (columnInfo == null) {
            throw new TiClientInternalException("ColumnRef is unbound");
        }
        return columnInfo;
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return  true;
        }

        if (another instanceof  TiColumnRef) {
            TiColumnRef tiColumnRef = (TiColumnRef) another;
            return tiColumnRef.columnInfo.getId() == this.columnInfo.getId() &&
               tiColumnRef.getName().equalsIgnoreCase(this.columnInfo.getName()) &&
               tiColumnRef.tableInfo.getId() == this.tableInfo.getId();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (int) (prime * result
                        + ((columnInfo == null) ? 0 : columnInfo.getId())
                        + ((tableInfo == null) ? 0 : tableInfo.getId()));
        return result;
    }
}
