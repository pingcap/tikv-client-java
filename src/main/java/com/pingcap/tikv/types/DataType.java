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
import com.google.common.collect.ImmutableMap;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

import java.util.List;
import java.util.Map;

import static com.pingcap.tikv.types.Flags.*;
import static com.pingcap.tikv.types.Types.*;

/**
 * Base Type for encoding and decoding TiDB row information.
 */
public class DataType {
     public enum EncodeType {
        KEY,
        VALUE
    }
    public static final int UNSPECIFIED_LEN = -1;

    // MySQL type
    protected int           tp;
    // Not Encode/Decode flag, this is used to strict mysql type
    // such as not null, timestamp
    protected int           flag;
    protected int           collation;
    protected int           length;
    private   List<String> elems;


   static final Map<Integer, String> typeNameMap = ImmutableMap.<Integer, String>builder()
            .put(NULL_FLAG, "NULL")
            .put(BYTES_FLAG, "STRING")
            .put(COMPACT_BYTES_FLAG, "STRING")
            .put(INT_FLAG, "INT")
            .put(UINT_FLAG, "UINT")
            .put(FLOATING_FLAG, "FLOAT")
            .put(DECIMAL_FLAG, "DECIMAL")
            .put(DURATION_FLAG, "Duration")
            .put(VARINT_FLAG, "VARINT")
            .put(UVARINT_FLAG, "UVARINT")
            .build();

    protected DataType(TiColumnInfo.InternalTypeHolder holder) {
        this.tp = holder.getTp();
        this.flag = holder.getFlag();
        this.length = holder.getFlen();
        this.collation = Collation.translate(holder.getCollate());
        this.elems = holder.getElems() == null ?
                ImmutableList.of() : holder.getElems();
    }

    protected DataType() {
        this.flag = 0;
        this.elems = ImmutableList.of();
        this.length = UNSPECIFIED_LEN;
        this.collation = Collation.DEF_COLLATION_CODE;
    }


    protected DataType(int tp) {
        this.tp = tp;
        this.flag = 0;
        this.elems = ImmutableList.of();
        this.length = UNSPECIFIED_LEN;
        this.collation = Collation.DEF_COLLATION_CODE;
    }

    protected DataType(int flag, int length, String collation, List<String> elems, int tp) {
        this.tp = tp;
        this.flag = flag;
        this.length = length;
        this.collation = Collation.translate(collation);
        this.elems = elems == null ? ImmutableList.of() : elems;
        this.tp = tp;
    }

    protected boolean isNullFlag(int flag) {
        return flag == NULL_FLAG;
    }

    /**
     * decode a null value from row which is nothing.
     * @param cdi source of data.
     * @param row destination of data
     * @param pos position of row.
     */
    public void decode(CodecDataInput cdi, Row row, int pos) {
    }

    /**
     * encode a Row to CodecDataOutput
     * @param cdo destination of data.
     * @param encodeType Key or Value.
     * @param value need to be encoded.
     */
    public void encode(CodecDataOutput cdo, EncodeType encodeType, Object value) {
        cdo.writeByte(NULL_FLAG);
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

    public void setFlag(int flag) {
        this.flag = flag;
    }
    public int getFlag() {
        return flag;
    }

    public List<String> getElems() {
        return this.elems;
    }

    public int getTypeCode() {
        return tp;
    }

   public static boolean hasNoDefaultFlag(int flag) {
       return (flag & NoDefaultValueFlag) > 0;
   }

   public static boolean hasAutoIncrementFlag(int flag) {
       return (flag & AutoIncrementFlag) > 0;
   }

   public static boolean hasUnsignedFlag(int flag) {
       return (flag & UnsignedFlag) > 0;
   }

   public static boolean hasZerofillFlag(int flag) {
       return (flag & ZerofillFlag) > 0;
   }

   public static boolean hasBinaryFlag(int flag) {
       return (flag & PriKeyFlag) > 0;
   }

   public static boolean hasUniKeyFlag(int flag) {
      return (flag & UniqueKeyFlag) > 0;
   }

   public static boolean hasMultipleKeyFlag(int flag) {
       return (flag & MultipleKeyFlag) > 0;
   }

   public static boolean hasTimestampFlag(int flag) {
       return (flag & TimestampFlag) > 0;
   }

   public static boolean hasOnUpdateNowFlag(int flag) {
       return (flag & OnUpdateNowFlag) > 0;
   }

   @Override
   public String toString() {
       return this.getClass().getSimpleName();
   }
}
