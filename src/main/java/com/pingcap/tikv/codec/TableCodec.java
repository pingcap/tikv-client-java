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
import com.pingcap.tikv.kv.Key;
import com.pingcap.tikv.util.Pair;

// Basically all protobuf ByteString involves buffer copy
// and might not be a good choice inside codec so we choose to
// return byte directly for future manipulation
// But this is not quite clean in case talking with GRPC interfaces
public class TableCodec {
    private static final int ID_LEN = 8;
    private static final int PREFIX_LEN = 1 + ID_LEN;
    public static final int RECORD_ROW_KEY_LEN = PREFIX_LEN + ID_LEN;

    private static final byte [] TBL_PREFIX      = new byte[] {'t'};
    private static final byte [] REC_PREFIX_SEP  = new byte[] {'_', 'r'};
    public static final byte [] IDX_PREFIX_SEP  = new byte[] {'_', 'i'};

    private static final long SIGN_MASK = ~Long.MAX_VALUE;

    // encodeRowKey encodes the table id and record handle into a bytes
    public static ByteString encodeRowKey(long tableId, byte[] encodeHandle) {
        Key buf = new Key();
        appendTableRecordPrefix(buf, tableId);
        buf.put(encodeHandle);
        return buf.toByteString();
    }

    private static void appendTableRecordPrefix(Key buf, long tableId) {
        buf.put(TBL_PREFIX);
        buf.writeLong(tableId);
//          LongUtils.writeLong(cdo, tableId);
        buf.put(REC_PREFIX_SEP);
    }

    // encodeRowKeyWithHandle encodes the table id, row handle into a bytes buffer/array
    public static ByteString encodeRowKeyWithHandle(long tableId, long handle) {
        Key buf = new Key();
        appendTableRecordPrefix(buf, tableId);
        buf.writeLong(handle);
        return buf.toByteString();
    }

    public static void encodeRecordKey(byte[] recordPrefix, long handle) {
        Key buf = new Key();
        buf.put(recordPrefix);
        buf.writeLong(handle);
    }

    public static Pair<Long, Long> decodeRecordKey(Key buf) {
        if (!consumeAndMatching(buf, TBL_PREFIX)) {
            throw new CodecException("Invalid Table Prefix");
        }
        long tableId = buf.readLong();

        if (!consumeAndMatching(buf, REC_PREFIX_SEP)) {
            throw new CodecException("Invalid Record Prefix");
        }

        long handle = buf.readLong();
        return Pair.create(tableId, handle);
    }

    public static long flipSignBit(long v) {
        return v ^ SIGN_MASK;
    }

    private static boolean consumeAndMatching(Key buf, byte[] prefix) {
        for (byte b : prefix) {
            if (buf.readByte() != b) {
                return false;
            }
        }
        return true;
    }

    // tablePrefix returns table's prefix 't'.
    public static byte[] tablePrefix() {
       return TBL_PREFIX;
    }
}
