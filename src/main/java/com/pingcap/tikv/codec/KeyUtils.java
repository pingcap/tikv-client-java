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

import com.google.protobuf.ByteString;

public class KeyUtils {
    private static final ByteString ZERO_BYTE = ByteString.copyFrom(new byte[]{0});
    public static ByteString getNextKeyInByteOrder(ByteString key) {
        return key.concat(ZERO_BYTE);
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
