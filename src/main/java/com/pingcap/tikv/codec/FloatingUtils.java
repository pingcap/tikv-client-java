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


public class FloatingUtils {
    public static final int FLOATING_FLAG = 5;

    /**
     * Decode as float
     * @param cdi source of data
     * @return decoded unsigned long value
     */
    public static double readDouble(CodecDataInput cdi) {
        long u = LongUtils.readULong(cdi);
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
