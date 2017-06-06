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

package com.pingcap.tikv.type;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * Base class for all integer types: Tiny, Short, Medium, Int, Long and LongLong
 */
public abstract class IntegerBaseType extends FieldType {
    private static int UNSIGNED_FLAG = 32;

    protected IntegerBaseType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected IntegerBaseType(boolean unsigned) {
        super(unsigned ? UNSIGNED_FLAG : 0);
    }

    public boolean isUnsigned() {
        return (flag & UNSIGNED_FLAG) != 0;
    }

    @Override
    public void decodeValueNoNullToRow(int flag, CodecDataInput cdi, Row row, int pos) {
        // NULL should be checked outside
        if (isUnsigned()) {
            if (flag == LongUtils.UVARINT_FLAG) {
                row.setULong(pos, LongUtils.readUVarLong(cdi));
            } else if (flag == LongUtils.UINT_FLAG) {
                row.setULong(pos, LongUtils.readULong(cdi));
            } else {
                throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
            }
        } else {
            if (flag == LongUtils.VARINT_FLAG) {
                row.setLong(pos, LongUtils.readVarLong(cdi));
            } else if (flag == LongUtils.INT_FLAG) {
                row.setLong(pos, LongUtils.readLong(cdi));
            } else {
                throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
            }
        }
    }

    @Override
    public abstract int getTypeCode();

    @Override
    public String toString() {
        return (isUnsigned() ? "Unsigned" : "Signed") + "_" + getClass().getSimpleName();
    }
}
