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

package com.pingcap.tikv.types.integer;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class MediumIntType extends IntegerBaseType<Integer> {
    public static final int TYPE_CODE = 9;

    public MediumIntType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Integer value) {
        row.setInteger(pos, value);
    }

    @Override
    public Integer decodeNotNull(int flag, CodecDataInput cdi) {
        return (int)decodeNotNullInternal(flag, cdi);
    }

    protected MediumIntType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static MediumIntType DEF_SIGNED_TYPE = new MediumIntType(false);
    public final static MediumIntType DEF_UNSIGNED_TYPE = new MediumIntType(true);
}