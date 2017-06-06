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

import com.pingcap.tikv.meta.TiColumnInfo;


public class BitType extends IntegerBaseType {
    public static final int TYPE_CODE = 16;

    public BitType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected BitType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static BitType DEF_SIGNED_TYPE = new BitType(false);
    public final static BitType DEF_UNSIGNED_TYPE = new BitType(true);
}
