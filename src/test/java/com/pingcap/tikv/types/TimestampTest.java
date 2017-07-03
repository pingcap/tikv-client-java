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

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static org.junit.Assert.assertEquals;

public class TimestampTest {
    @Test
    public void fromPackedLongAndToPackedLongTest() {
        LocalDateTime time = LocalDateTime.of(1999, 12, 12, 1, 1, 1, 1);
        LocalDateTime time1 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time));
        assertEquals(time, time1);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        time = LocalDateTime.parse("2010-10-10 10:11:11", formatter);
        time1 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time));
        assertEquals(time, time1);
    }
}
