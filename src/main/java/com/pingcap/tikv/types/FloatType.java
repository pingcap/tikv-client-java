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

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.FloatingUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class FloatType extends FloatingBaseType<Float> {
    public static final int TYPE_CODE = 4;

    public FloatType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    public FloatType() {}

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Float value) {
        row.setFloat(pos, value);
    }

    @Override
    public Float decodeNotNull(int flag, CodecDataInput cdi) {
        return (float)decodeNotNullInternal(flag, cdi);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static FloatType DEF_TYPE = new FloatType();
}