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

import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public abstract class StringBaseType extends FieldType {
    // mysql/type.go:34
    public static final int TYPE_CODE = 15;
    public StringBaseType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    protected StringBaseType() {}

    @Override
    protected void decodeValueNoNullToRow(int flag, CodecDataInput cdi, Row row, int pos) {
        if (flag == BytesUtils.COMPACT_BYTES_FLAG) {
           String v = new String(BytesUtils.readCompactBytes(cdi));
           row.setString(pos, v);
        } else if (flag == BytesUtils.BYTES_FLAG) {
           String v = new String(BytesUtils.readBytes(cdi));
           row.setString(pos, v);
        } else {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
    }

    public int getTypeCode() {
        return TYPE_CODE;
    }
}
