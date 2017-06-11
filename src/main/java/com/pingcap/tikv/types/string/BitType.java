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

package com.pingcap.tikv.types.string;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.types.FieldType;


public class BitType extends FieldType<Long> {
    public static final int TYPE_CODE = 16;

    public BitType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected BitType() {}

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Long value) {
        row.setLong(pos, value);
    }

    @Override
    public Long decodeNotNull(int flag, CodecDataInput cdi) {
        if (flag == LongUtils.UVARINT_FLAG) {
            return LongUtils.readUVarLong(cdi);
        } else if (flag == LongUtils.UINT_FLAG) {
            return LongUtils.readULong(cdi);
        } else {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static BitType DEF_TYPE = new BitType();
}
