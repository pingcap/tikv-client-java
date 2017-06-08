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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

// Flags only used when we need encode/decode.
public class Flags {
    static final int NULL_FLAG = 0;
    static final int BYTES_FLAG = 1;
    static final int COMPACT_BYTES_FLAG = 2;
    static final int INT_FLAG = 3;
    static final int UINT_FLAG = 4;
    static final int FLOATING_FLAG = 5;
    static final int DECIMAL_FLAG = 6;
    static final int DURATION_FLAG = 7;
    static final int VARINT_FLAG = 8;
    static final int UVARINT_FLAG = 9;
    private static final int JSON_FLAG = 10;
    private static final int MAX_FLAG = 250;

    private static final Set<Integer> flagsSet = ImmutableSet.<Integer>builder()
            .add(NULL_FLAG)
            .add(BYTES_FLAG)
            .add(COMPACT_BYTES_FLAG)
            .add(INT_FLAG)
            .add(UINT_FLAG)
            .add(FLOATING_FLAG)
            .add(DECIMAL_FLAG)
            .add(DURATION_FLAG)
            .add(VARINT_FLAG)
            .add(UVARINT_FLAG)
            .add(JSON_FLAG)
            .add(MAX_FLAG)
            .build();

    public boolean isValidFlag(int flag) {
        return flagsSet.contains(flag);
    }
}
