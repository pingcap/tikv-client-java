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
    public static double readDecimalFully(CodecDataInput cdi) {
        int precision = cdi.readUnsignedByte();
        int frac = cdi.readUnsignedByte();
        List<Integer> data = new ArrayList<>();
        for(;!cdi.eof();) {
            data.add(cdi.readUnsignedByte());
        }

        if (data.size() < 3) {
            throw new IllegalArgumentException("insufficient bytes to read value");
        }

        MyDecimal dec = new MyDecimal();
        dec.fromBin(precision, frac, Ints.toArray(data));
        return dec.toDecimal();
    }

    public static void writeDecimalFully(CodecDataOutput cdo, double lvalue) {
        String value = Double.toString(lvalue);
        MyDecimal dec = new MyDecimal();
        dec.fromString(value);
        int[] data = dec.toBin(dec.precision(), dec.frac());
        cdo.writeChar(dec.precision());
        cdo.writeChar(dec.frac());
        for (int aData : data) {
            cdo.writeChar(aData);
        }
    }
}