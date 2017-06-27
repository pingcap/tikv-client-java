/*
 *
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
 *
 */

package com.pingcap.tikv.operation.transformer;

import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;

public class Cast extends NoOp {
    public Cast(DataType type) {
        super(type);
    }

    @Override
    public void append(Object value, Row row, int pos) {
        Object casted;
        if (value == null) {
            row.set(row.fieldCount(), targetDataType, null);
        }
        if (targetDataType instanceof IntegerType) {
            casted = castToLong(value);
        } else if (targetDataType instanceof BytesType) {
            casted = castToString(value);
        } else if (targetDataType instanceof DecimalType) {
            casted = castToDouble(value);
        } else {
            throw new UnsupportedOperationException("only support cast to Long, Double and String");
        }
        row.set(pos, targetDataType, casted);
    }

    public Double castToDouble(Object obj) {
        if (obj instanceof Number) {
            Number num = (Number)obj;
            return num.doubleValue();
        }
        throw new UnsupportedOperationException("can not cast un-number to double ");
    }

    public Long castToLong(Object obj) {
        if (obj instanceof Number) {
            Number num = (Number)obj;
            return num.longValue();
        }
        throw new UnsupportedOperationException("can not cast un-number to long ");
    }

    public String castToString(Object obj) {
        return obj.toString();
    }
}
