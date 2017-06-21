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

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;

import java.util.Arrays;

public class KeyUtils {
    private static final ByteString ZERO_BYTE = ByteString.copyFrom(new byte[]{0});

    public static ByteString getNextKeyInByteOrder(ByteString key) {
        return key.concat(ZERO_BYTE);
    }

    public static byte[] getNextKeyInByteOrder(byte[] key) {
        return Arrays.copyOf(key, key.length + 1);
    }

    /**
     * returns the next prefix key.
     * Original bytes will be reused if possible
     * @param key key to encode
     * @return
     */
    public static byte[] prefixNext(byte[] key) {
        int i;
        for (i = key.length - 1; i >= 0; i--) {
            if (key[i] != UnsignedBytes.MAX_VALUE) {
                key[i] ++;
                break;
            }
        }
        if (i == -1) {
            return getNextKeyInByteOrder(key);
        }
        return key;
    }

    public static boolean hasPrefix(ByteString str, ByteString prefix) {
        for (int i = 0; i < prefix.size(); i++) {
            if (str.byteAt(i) != prefix.byteAt(i)) {
                return false;
            }
        }
        return true;
    }
}
