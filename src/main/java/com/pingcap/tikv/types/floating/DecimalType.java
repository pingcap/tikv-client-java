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

package com.pingcap.tikv.types.floating;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.DecimalUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.types.FieldType;

public class DecimalType extends FieldType<Double> {
    public static final int TYPE_CODE = 0;
    private static final int DECIMAL_FLAG = 6;

    public DecimalType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected DecimalType() {}

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Double value) {
        row.setDouble(pos, value);
    }

    @Override
    public Double decodeNotNull(int flag, CodecDataInput cdi) {
        if (flag != DECIMAL_FLAG) {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
        return DecimalUtils.readDecimalFully(cdi);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public static final DecimalType DEF_TYPE = new DecimalType();
}
