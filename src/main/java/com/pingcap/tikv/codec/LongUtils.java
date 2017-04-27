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

package com.pingcap.tikv.codec;

import com.pingcap.tikv.TiClientInternalException;

public class LongUtils {
    public static final byte INT_FLAG = 3;
    public static final byte UINT_FLAG = 4;
    public static final byte VARINT_FLAG = 8;
    public static final byte UVARINT_FLAG = 9;

    public static void writeLongFull(CodecDataOutput cdo, long lVal, boolean comparable) {
        if (comparable) {
            cdo.writeByte(INT_FLAG);
            writeLong(cdo, lVal);
        } else {
            cdo.writeByte(VARINT_FLAG);
            writeVarLong(cdo, lVal);
        }
    }

    public static void writeULongFull(CodecDataOutput cdo, long lVal, boolean comparable) {
        if (comparable) {
            cdo.writeByte(UINT_FLAG);
            writeULong(cdo, lVal);
        } else {
            cdo.writeByte(UVARINT_FLAG);
            writeUVarLong(cdo, lVal);
        }
    }

    public static void writeLong(CodecDataOutput cdo, long lVal) {
        cdo.writeLong(CodecUtil.flipSignBit(lVal));
    }

    public static void writeULong(CodecDataOutput cdo, long lVal) {
        cdo.writeLong(lVal);
    }

    public static void writeVarLong(CodecDataOutput cdo, long value) {
        long sign = value << 1;
        if (value < 0) {
            sign = ~sign;
        }
        writeUVarLong(cdo, sign);
    }

    public static void writeUVarLong(CodecDataOutput cdo, long value) {
        while ((value - 0x80) >= 0) {
            cdo.writeByte((byte)value | 0x80);
            // logical shift as unsigned long
            value >>>= 7;
        }
        cdo.writeByte((byte)value);
    }

    public static long readLongFully(CodecDataInput cdi) {
        byte flag = cdi.readByte();

        switch (flag) {
            case INT_FLAG:
                return readLong(cdi);
            case VARINT_FLAG:
                return readVarLong(cdi);
            default:
                throw new TiClientInternalException("Invalid Flag type for signed long type: " + flag);
        }
    }

    public static long readULongFully(CodecDataInput cdi) {
        byte flag = cdi.readByte();
        switch (flag) {
            case UINT_FLAG:
                return readULong(cdi);
            case UVARINT_FLAG:
                return readUVarLong(cdi);
            default:
                throw new TiClientInternalException("Invalid Flag type for unsigned long type: " + flag);
        }
    }

    public static long readLong(CodecDataInput cdi) {
        return CodecUtil.flipSignBit(cdi.readLong());
    }

    public static long readULong(CodecDataInput cdi) {
        return cdi.readLong();
    }

    public static long readVarLong(CodecDataInput cdi) {
        long ux = readUVarLong(cdi);
        // shift as unsigned
        long x = ux >>> 1;
        if ((ux & 1) != 0) {
            x = ~x;
        }
        return x;
    }

    public static long readUVarLong(CodecDataInput cdi) {
        long x = 0;
        int s = 0;
        for (int i = 0; !cdi.eof(); i++) {
            long b = cdi.readUnsignedByte();
            if ((b - 0x80) < 0) {
                if (i > 9 || i == 9 && b > 1) {
                    throw new InvalidCodecFormatException("readUVarLong overflow");
                }
                return x | b << s;
            }
            x |= (b & 0x7f) << s;
            s += 7;
        }
        throw new InvalidCodecFormatException("readUVarLong encountered unfinished data");
    }
}
