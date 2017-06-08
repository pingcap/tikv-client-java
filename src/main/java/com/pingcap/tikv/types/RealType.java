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

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

import static com.pingcap.tikv.types.Flags.FLOATING_FLAG;

public class RealType extends DataType {
    private static final long signMask = 0x8000000000000000L;

    public RealType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }
    public static RealType of(int tp) {
       return new RealType(tp);
    }

    private RealType(int tp) {
        super(tp);
    }

    /**
     * decode value from cdi to row per tp.
     * @param cdi source of data.
     * @param row destination of data
     * @param pos position of row.
     */
    @Override
    public void decode(CodecDataInput cdi, Row row, int pos) {
        // check flag first and then read.
        int flag = cdi.readUnsignedByte();
        if (flag != FLOATING_FLAG) {
            throw new InvalidCodecFormatException("Invalid Flag type for float type: " + codecMap.get(flag));
        }
        long u = IntegerType.readULong(cdi);
        if((u & signMask) > 0) {
            // origin u &= ^signMask in golang
            // ^ is known as bitwise complement.
            // is m ^ x  with m = "all bits set to 1" for unsigned x
            // and  m = -1 for signed x
            u &= ~signMask;
        } else {
            // u = ^u
            u = ~u;
        }
        float val =  Float.intBitsToFloat((int)u);
        row.setDouble(pos, val);
    }

    /**
     * encode a value to cdo.
     * @param cdo destination of data.
     * @param encodeType Key or Value.
     * @param value need to be encoded.
     */
    @Override
    public void encode(CodecDataOutput cdo, EncodeType encodeType, Object value) {
        float val;
        if(value instanceof  Float) {
            val = ((Float)value).floatValue();
        } else {
            throw new UnsupportedOperationException("Can not cast Un-number to Float");
        }
        long bits = Float.floatToIntBits(val);
        if (bits > 0) {
            bits |= signMask;
        } else {
            // u = ^u;
            bits = ~bits;
        }
        IntegerType.writeULong(cdo, bits);
    }

    @Override
    public String toString() {
        return "ClassReal";
    }


    /**
     * Decode as float
     * @param cdi source of data
     * @return decoded unsigned long value
     */
    public static double readDouble(CodecDataInput cdi) {
        long u = IntegerType.readULong(cdi);
        if (u < 0) {
            u &= Long.MAX_VALUE;
        } else {
            u = ~u;
        }
        return Double.longBitsToDouble(u);
    }

    /**
     * Encoding a double value to byte buffer
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeDouble(CodecDataOutput cdo, double val) {
        throw new UnsupportedOperationException();
    }

    /**
     * Encoding a float value to byte buffer
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeFloat(CodecDataOutput cdo, float val) {
        throw new UnsupportedOperationException();
    }
}
