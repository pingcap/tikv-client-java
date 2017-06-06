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

public class DoubleType extends FloatingType {
    public static final int TYPE_CODE = 5;

    public DoubleType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    public DoubleType() {}

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static DoubleType DEF_TYPE = new DoubleType();
}
