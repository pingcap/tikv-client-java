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

import com.pingcap.tikv.types.FieldType;
import com.pingcap.tikv.types.floating.DoubleType;
import com.pingcap.tikv.types.integer.IntegerBaseType;
import com.pingcap.tikv.types.string.StringBaseType;

public class Cast extends NoOp {
    public Cast(FieldType fieldType) {
        super(fieldType);
    }

    @Override
    public Object apply(Object obj) {
        if (targetType instanceof IntegerBaseType) {
            return castToLong(obj);
        } else if (targetType instanceof StringBaseType) {
            return castToString(obj);
        } else if (targetType instanceof DoubleType) {
            return castToDouble(obj);
        } else {
            throw new UnsupportedOperationException("only support cast to Long, Double and String");
        }
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
