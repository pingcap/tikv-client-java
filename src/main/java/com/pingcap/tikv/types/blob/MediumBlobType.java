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

package com.pingcap.tikv.types.blob;

import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.types.string.StringBaseType;

public class MediumBlobType extends StringBaseType {
    public static final int TYPE_CODE = 0xfa;

    public MediumBlobType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    protected MediumBlobType() {}

    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static MediumBlobType DEF_TYPE = new MediumBlobType();
}
