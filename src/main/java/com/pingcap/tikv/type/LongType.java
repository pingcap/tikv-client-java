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

import com.pingcap.tikv.kv.Key;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

import static com.pingcap.tikv.codec.Encoder.*;

public class LongType extends FieldType {
    private final boolean varLength;

    private static int UNSIGNED_FLAG = 32;
    public static final int TYPE_CODE = 3;

    public static final LongType DEF_VLONG = new LongType();

    public LongType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
        this.varLength = true;
    }

    public LongType() {
        this.varLength = true;
    }

    public LongType(int flag, boolean varLength) {
        this.varLength = varLength;
    }

    protected boolean isUnsigned() {
        return (flag & UNSIGNED_FLAG) != 0;
    }

    @Override
    public void decodeValueNoNullToRow(Key key, Row row, int pos) {
        // NULL should be checked outside
        if (isUnsigned()) {
            if (varLength) {
//                row.setULong(pos, readUVarLong());
            } else {
//                row.setULong(pos, readULong());
            }
        } else {
            if (varLength) {
//                row.setLong(pos, readVarLong());
            } else {
//                row.setLong(pos, readLong());
            }
        }
    }



    @Override
    protected boolean isValidFlag(int flag) {
        if (isUnsigned()) {
            return flag == UINT_FLAG || flag == UVARINT_FLAG;
        } else {
            return flag == INT_FLAG || flag == VARINT_FLAG;
        }
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    @Override
    public String toString() {
        return (isUnsigned() ? "Unsigned" : "Signed") + "_LongType";
    }
}
