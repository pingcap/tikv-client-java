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

import com.pingcap.tikv.kv.Key;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.codec.Codec.Decoder.decodeBytes;
import static com.pingcap.tikv.codec.Codec.Decoder.decodeLong;

public class Codec {
    private static final byte NIL_FLAG = 0;
    private static final byte BYTES_FLAG = 1;
    private static final byte COMPACT_BYTES_FLAG = 2;
    private static final byte INT_FLAG = 3;
    private static final byte UINT_FLAG = 4;
    private static final byte FLOAT_FLAG = 5;
    private static final byte DECIMAL_FLAG = 6;
    private static final byte DURATION_FLAG = 7;
    private static final byte VARINT_FLAG = 8;
    private static final byte UVARINT_FLAG = 9;
    private static final byte MAX_FLAG = (byte) 250;
    private static final long SIGN_MASK = ~Long.MAX_VALUE;

    // Folloing variables are used for decoding bytes
    private static final int GRP_SIZE = 8;
    private static final byte[] PADS = new byte[GRP_SIZE];
    private static final byte MARKER = (byte) 0xFF;
    private static final byte PAD = (byte) 0x0;

    public static List<Object> decode(byte[] buf, int size) {
        checkArgument(buf.length > 1);

        int start = 0;
        List<Object> res = new ArrayList<>();
        for(; start < buf.length;) {
            res.add(decodeOne(buf, start));
            // update start. Decode 8 bytes together
            start += GRP_SIZE;
        }

        return res;
    }

    // TODO finish this later. Maybe we need define a type that wrapper
    public static Long decodeOne(byte[] buf, int start) {
        int flag = (int) buf[0];
        switch (flag) {
            // Unlike go, java doest have to unsigned int. Even int32 int64 is
            // only in go.
            // Basically, all date the folowing flag are converted to long
            // instead.
            case INT_FLAG :
            case UINT_FLAG :
            case VARINT_FLAG :
            case UVARINT_FLAG :
                return decodeLong(buf, 0);
            case FLOAT_FLAG :
            case BYTES_FLAG :
                decodeBytes(buf);
            case COMPACT_BYTES_FLAG :
                decodeLong(buf, 0);
            case DECIMAL_FLAG :
                decodeLong(buf, 0);
            case DURATION_FLAG :
                decodeLong(buf, 0);
            case NIL_FLAG :
            default :
                throw new CodecException("invalid encoded key flag: " + flag);
        }
    }

    public static class Decoder {
        /**
         * decodeInt decodes value encoded by EncodeInt before. for memory
         * comparable
         *
         * @param buf
         *            source of data
         * @param start
         *            is the index where to start to read 8 bytes
         * @return decoded signed long value
         */
        public static long decodeLong(byte[] buf, int start) {
            if (buf.length > (start + 8)) {
                throw new InvalidCodecFormatException("insufficient bytes to decode value");
            }
            return flipSignBitIfNeeded(byteToLong(buf, start));
        }

        /**
         * decodeIntDesc decodes value encoded by EncodeInt before. It returns
         * the leftover un-decoded slice, decoded value if no error.
         *
         * @param buf
         *            source of data
         * @param start
         *            is the index where to start to read 8 bytes
         * @return decoded signed long value
         */
        public static long decodeLongDesc(byte[] buf, int start) {
            if (buf.length > (start + 8)) {
                throw new InvalidCodecFormatException("insufficient bytes to decode value");
            }
            return flipSignBitIfNeeded(~byteToLong(buf, start));
        }

        /*
         * byteToLong parse bytes into a long value. The size of buf shoould be
         * checked outside.
         *
         * @param buf represents source data to be parsed.
         */
        private static long byteToLong(byte[] buf, int start) {
            return (((long) buf[start] << 56) +
                    ((long) (buf[start + 1] & 255) << 48) +
                    ((long) (buf[start + 2] & 255) << 40) +
                    ((long) (buf[start + 3] & 255) << 32) +
                    ((long) (buf[start + 4] & 255) << 24) +
                    ((buf[start + 5] & 255) << 16) +
                    ((buf[start + 6] & 255) << 8) +
                    ((buf[start + 7] & 255) << 0));
        }

        /**
         * Decode as unsigned long without any binary manipulation
         *
         * @param buf
         *            source of data
         * @return decoded unsigned long value
         */
        public static long decodeULong(byte[] buf, int start) {
            return decodeLong(buf, start);
        }

        /**
         * Decode as var-length long, the same as go's binary.Varint
         *
         * @param buf
         *            source of data
         * @return decoded signed long value
         */
        public static long decodeVarLong(byte[] buf, int start) {
            long ux = decodeUVarLong(buf, start);
            long x = ux >>> 1;
            if ((ux & 1) != 0) {
                x = ~x;
            }
            return x;
        }

        /**
         * Decode as var-length unsigned long, the same as go's binary.Uvarint
         *
         * @param buf
         *            source of data
         * @return decoded unsigned long value
         */
        public static long decodeUVarLong(byte[] buf, int start) {
            long x = 0;
            int s = 0;
            for (int i = 0; i < buf.length; i++) {
                long b = (long) buf[i];
                if ((b - 0x80) < 0) {
                    if (i > 9 || i == 9 && b > 1) {
                        throw new InvalidCodecFormatException("decodeUVarLong overflow");
                    }
                    return x | b << s;
                }
                x |= (b & 0x7f) << s;
                s += 7;
            }
            throw new InvalidCodecFormatException("decodeUVarLong encountered unfinished data");
        }

        public static Object[] decode(Key key) {
            return null;
        }

        public static long flipSignBitIfNeeded(long u) {
            return u ^ SIGN_MASK;
        }

        private static void copyFromSrcToDst(byte[] src, byte[] dst, int from, int to) {
            System.arraycopy(src, from, dst, from, to - from);
        }

        public static byte[] decodeBytes(byte[] buf) {
            return decodeBytes(buf, false);
        }

        private static byte[] decodeBytes(byte[] buf, boolean reverse) {
            byte[] data = new byte[buf.length];
            int start = 0;
            while (true) {
                byte[] groupBytes = new byte[GRP_SIZE + 1];
                Arrays.copyOfRange(buf, 0, GRP_SIZE);
                byte[] group = Arrays.copyOfRange(groupBytes, start, GRP_SIZE + start);

                byte padCount;
                byte marker = groupBytes[GRP_SIZE];

                if (reverse) {
                    padCount = marker;
                } else {
                    padCount = (byte) (MARKER - marker);
                }

                // TODO: supply a msg explain condition when this check failed.
                // Reason: invalid marker byte, group bytes is groupBytes.
                checkArgument(padCount <= GRP_SIZE);
                int realGroupSize = GRP_SIZE - padCount;
                copyFromSrcToDst(buf, data, start, realGroupSize + start);
                // update start point for querying data from buf.
                start = start + GRP_SIZE + 1;

                if (padCount != 0) {
                    byte padByte = PAD;
                    if (reverse) {
                        padByte = (byte) MARKER;
                    }
                    // Check validity of padding bytes.
                    for (int i = realGroupSize; i < group.length; i++) {
                        checkArgument(padByte == group[i]);
                    }
                    break;
                }
            }
            if (reverse) {
                for (int i = 0; i < data.length; i++) {
                    data[i] = (byte) ~data[i];
                }
            }
            return data;
        }
    }

    public static class Encoder {
        /**
         * Encoding a long value to byte buffer with type flag at the beginning
         * 
         * @param lVal
         *            The data to encode
         * @param comparable
         *            If the output should be memory comparable without
         *            decoding. In real TiDB use case, if used in Key encoding,
         *            we output memory comparable format otherwise not
         */
        public static byte[] encodeLong(long lVal, boolean comparable) {
            Key data = new Key();
            if (comparable) {
                data.put(INT_FLAG);
                data.put(encodeLong(lVal));
            } else {
                data.put(VARINT_FLAG);
                encodeVarLong(lVal);
            }
            return data.toByteArray();
        }

        /**
         * Encoding a unsigned long value to byte buffer with type flag at the
         * beginning
         * 
         * @param lVal
         *            The data to encode, note that long is treated as unsigned
         * @param comparable
         *            If the output should be memory comparable without
         *            decoding. In real TiDB use case, if used in Key encoding,
         *            we output memory comparable format otherwise not
         */
        public static void encodeULongFull(long lVal, boolean comparable) {
            Key data = new Key();
            if (comparable) {
                data.put(UINT_FLAG);
                data.put(encodeULong(lVal));
            } else {
                data.put(UVARINT_FLAG);
                data.put(encodeUVarLong(lVal));
            }
        }

        /**
         * Encode long value without type flag at the beginning The signed bit
         * is flipped for memory comparable purpose
         * 
         * @param v
         *            The data to encode
         */
        public static byte[] encodeLong(long v) {
            // EncodeRowKey encodes the table id and record handle into a kv.Key
            v = TableCodec.flipSignBit(v);
            byte[] data = new byte[8];
            data[0] = (byte) (v >>> 56);
            data[1] = (byte) (v >>> 48);
            data[2] = (byte) (v >>> 40);
            data[3] = (byte) (v >>> 32);
            data[4] = (byte) (v >>> 24);
            data[5] = (byte) (v >>> 16);
            data[6] = (byte) (v >>> 8);
            data[7] = (byte) (v);
            return data;
        }

        /**
         * Encode long value without type flag at the beginning
         * 
         * @param lVal
         *            The data to encode
         */
        public static byte[] encodeULong(long lVal) {
            return encodeLong(lVal);
        }

        /**
         * Encode var-length long, same as go's binary.PutVarint
         * 
         * @param value
         *            The data to encode
         */
        public static byte[] encodeVarLong(long value) {
            long ux = value << 1;
            if (value < 0) {
                ux = ~ux;
            }
            return encodeUVarLong(ux);
        }

        /**
         * Encode Data as var-length long, the same as go's binary.PutUvarint
         * 
         * @param value
         *            The data to encode
         */
        public static byte[] encodeUVarLong(long value) {
            Key data = new Key();
            while ((value - 0x80) >= 0) {
                data.put((byte) (value | 0x80));
                value >>>= 7;
            }
            data.put((byte) value);
            return data.toByteArray();
        }
    }
}
