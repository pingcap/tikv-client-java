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
import com.pingcap.tikv.codec.FloatingUtils;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * Base class for Float and Double
 */
public abstract class FloatingType extends FieldType {
    protected FloatingType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    protected FloatingType() {}

    @Override
    public void decodeValueNoNullToRow(int flag, CodecDataInput cdi, Row row, int pos) {
        if (flag == FloatingUtils.FLOATING_FLAG) {
            double v = FloatingUtils.readDouble(cdi);
            row.setDecimal(pos, v);
        } else {
            throw new TiClientInternalException("Invalid " + toString() + " flag: " + flag);
        }
    }
}
