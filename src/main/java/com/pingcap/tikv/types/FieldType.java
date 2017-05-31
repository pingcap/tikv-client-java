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


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

import java.util.List;

public abstract class FieldType<T> {
    protected static final byte   NULL_FLAG = 0;
    protected static final int    UNSPECIFIED_LEN = -1;

    protected final int           flag;
    protected final int           collation;
    protected final int           length;
    private   final List<String>  elems;

    protected FieldType(TiColumnInfo.InternalTypeHolder holder) {
        this.flag = holder.getFlag();
        this.length = holder.getFlen();
        this.collation = Collation.translate(holder.getCollate());
        this.elems = holder.getElems() == null ?
                ImmutableList.of() : holder.getElems();
    }

    protected FieldType() {
        this.flag = 0;
        this.elems = ImmutableList.of();
        this.length = UNSPECIFIED_LEN;
        this.collation = Collation.DEF_COLLATION_CODE;
    }

    protected FieldType(int flag) {
        this.flag = flag;
        this.elems = ImmutableList.of();
        this.length = UNSPECIFIED_LEN;
        this.collation = Collation.DEF_COLLATION_CODE;
    }

    protected FieldType(int flag, int length, String collation, List<String> elems) {
        this.flag = flag;
        this.length = length;
        this.collation = Collation.translate(collation);
        this.elems = elems == null ? ImmutableList.of() : elems;
    }

    protected abstract void decodeValueNoNullToRow(Row row, int pos, T value);

    public T decode(int flag, CodecDataInput cdi) {
        if (isNullFlag(flag)) {
            return null;
        }
        return decodeNotNull(flag, cdi);
    }

    public abstract T decodeNotNull(int flag, CodecDataInput cdi);

    public void decodeValueToRow(CodecDataInput cdi, Row row, int pos) {
        int flag = cdi.readUnsignedByte();
        if (isNullFlag(flag)) {
            row.setNull(pos);
        }
        decodeValueNoNullToRow(row, pos, decodeNotNull(flag, cdi));
    }

    protected boolean isNullFlag(int flag) {
        return flag == NULL_FLAG;
    }

    public int getCollationCode() {
        return collation;
    }

    public int getLength() {
        return length;
    }

    public int getDecimal() {
        return UNSPECIFIED_LEN;
    }

    public int getFlag() {
        return flag;
    }

    public List<String> getElems() {
        return this.elems;
    }

    public abstract int getTypeCode();

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
