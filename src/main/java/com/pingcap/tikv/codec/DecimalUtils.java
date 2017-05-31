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

import java.util.stream.Stream;
import java.util.Arrays;
import java.io.IOException;
import java.lang.Byte;
import javax.sound.midi.SysexMessage;
import java.util.ArrayList;
import java.util.List;
import com.pingcap.tikv.codec.MyDecimal;
import com.google.common.primitives.Ints;

public class DecimalUtils {
    /** read a decimal value from CodecDataInput
     * @param cdi cdi is source data.
     * */
    public static double readDecimalFully(CodecDataInput cdi) {
        if (cdi.available() < 3) {
            throw new IllegalArgumentException("insufficient bytes to read value");
        }

        MyDecimal dec = new MyDecimal();
        List<Integer> data = new ArrayList<>();
        int precision = cdi.readUnsignedByte();
        int frac = cdi.readUnsignedByte();
        int length = precision + frac;
        int curPos = cdi.size() - cdi.available();
        for(int i = 0; i < length; i++) {
            if (cdi.eof()){
                break;
            }
            data.add(cdi.readUnsignedByte());
        }

        int binSize = dec.fromBin(precision, frac, Ints.toArray(data));
        cdi.mark(curPos+binSize);
        cdi.reset();
        return dec.toDecimal();
    }

    /** write a decimal value from CodecDataInput
     * @param cdo cdo is destination data.
     * @param lvalue is decimal value that will be written into cdo.
     * */
    public static void writeDecimalFully(CodecDataOutput cdo, double lvalue) {
        MyDecimal dec = new MyDecimal();
        dec.fromDecimal(lvalue);
        int[] data = dec.toBin(dec.precision(), dec.frac());
        cdo.writeByte(dec.precision());
        cdo.writeByte(dec.frac());
        for (int aData : data) {
            cdo.writeByte(aData & 0xFF);
        }
    }
}