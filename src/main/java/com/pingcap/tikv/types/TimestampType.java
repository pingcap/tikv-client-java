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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;

public class TimestampType extends DataType {
    static TimestampType of(int tp) {
        return new TimestampType(tp);
    }

    private TimestampType(int tp) {
        super(tp);
    }

    @Override
    public Object decodeNotNull(int flag, CodecDataInput cdi) {
        // MysqlTime MysqlDate MysqlDatetime
        if (flag == UVARINT_FLAG) {
            // read packedUint
            LocalDateTime localDateTime = fromPackedLong(IntegerType.readUVarLong(cdi));
            return Timestamp.valueOf(localDateTime);
        // row.setTimestamp(pos, timestamp);
        } else if (flag == INT_FLAG){
            long nanoSec = IntegerType.readLong(cdi);
            Duration duration = Duration.ofNanos(nanoSec);
            // Go and Java share the same behavior. Time is calculated from 1970 Jan 1 UTC.
            // row.setTime(pos, time);
            return new Time(duration.toMillis());
        } else {
            throw new InvalidCodecFormatException("Invalid Flag type for TimestampType: " + flag);
        }
    }

    /**
     * decode a value from cdi to row per tp.
     * @param cdi source of data.
     * @param row destination of data
     * @param pos position of row.
     */
    public void decode(CodecDataInput cdi, Row row, int pos) {
        int flag = cdi.readUnsignedByte();
        // MysqlTime MysqlDate MysqlDatetime
        if (flag == UVARINT_FLAG) {
            // read packedUint
            LocalDateTime localDateTime = fromPackedLong(IntegerType.readUVarLong(cdi));
            Timestamp timestamp = Timestamp.valueOf(localDateTime);
            row.setTimestamp(pos, timestamp);
        } else if (flag == INT_FLAG){
            long nanoSec = IntegerType.readLong(cdi);
            Duration duration = Duration.ofNanos(nanoSec);
            // Go and Java share the same behavior. Time is calculated from 1970 Jan 1 UTC.
            Time time = new Time(duration.toMillis());
            row.setTime(pos, time);
        } else {
            throw new InvalidCodecFormatException("Invalid Flag type for TimestampType: " + flag);
        }
    }

    /**
     * encode a value to cdo per type.
     * @param cdo destination of data.
     * @param encodeType Key or Value.
     * @param value need to be encoded.
     */
    @Override
    public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
        LocalDateTime localDateTime;
        // TODO, is LocalDateTime enough here?
        if (value instanceof LocalDateTime) {
            localDateTime = (LocalDateTime)value;
        } else {
            throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
        }
        long val =  toPackedLong(localDateTime);
        IntegerType.writeULong(cdo, val);
    }

    /**
     * Encode a LocalDateTime to a packed long.
     * @param time localDateTime that need to be encoded.
     * @return a packed long.
     */
    public static long toPackedLong(LocalDateTime time) {
        int year = time.getYear();
        int month = time.getMonthValue();
        int day = time.getDayOfMonth();
        int hour = time.getHour();
        int minute = time.getMinute();
        int second = time.getSecond();
        // 1 microsecond = 1000 nano second
        int micro = time.getNano() / 1000;
        int ymd = (year*13 + month) << 5 | day;
        int hms = hour << 12 | minute << 6 | second;
        return ((ymd << 17 | hms) << 24) | micro;
    }

    /**
     * Decode a packed long to LocalDateTime.
     * @param packed a long value
     * @return a decoded LocalDateTime.
     */
    public static LocalDateTime fromPackedLong(long packed) {
        long ymdhms = packed >> 24;
        long ymd = ymdhms >> 17;
        int day = (int)(ymd & (1 << 5 -1));
        long ym = ymd >> 5;
        int month = (int)(ym % 13);
        int year = (int)(ym/13);

        int hms = (int)(ymdhms & (1<<17-1));
        int second = hms & (1<<6 - 1);
        int minute = (hms >> 6) & (1<<6 - 1);
        int hour = hms >> 12;
        int microsec = (int)(packed % (1 << 24));
        return LocalDateTime.of(year, month, day, hour, minute, second, microsec);
    }
}
