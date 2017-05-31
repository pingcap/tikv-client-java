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

import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public abstract class StringBaseType extends FieldType<String> {
    // mysql/type.go:34
    public static final int TYPE_CODE = 15;
    public StringBaseType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    protected StringBaseType() {}

    public String decodeNotNull(int flag, CodecDataInput cdi) {
        if (flag == BytesUtils.COMPACT_BYTES_FLAG) {
            return new String(BytesUtils.readCompactBytes(cdi));
        } else if (flag == BytesUtils.BYTES_FLAG) {
            return new String(BytesUtils.readBytes(cdi));
        } else {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, String value) {
        row.setString(pos, value);
    }

    public int getTypeCode() {
        return TYPE_CODE;
    }
}