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

package com.pingcap.tikv.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTimeUtil {
  @Test
  public void testConversion() {
    long[] cases = {
        1513601777L,
        978307200000L,
        946684799000L,
        946684800000L,
        1078012800000L,
        815616000000L,
    };

    TimeUtil.Date[] results = {
        new TimeUtil.Date(1970, 1, 18),
        new TimeUtil.Date(2001, 1, 1),
        new TimeUtil.Date(1999, 12, 31),
        new TimeUtil.Date(2000, 1, 1),
        new TimeUtil.Date(2004, 2, 29),
        new TimeUtil.Date(1995, 11, 6),
    };

    assertEquals(cases.length, results.length);
    for (int i = 0; i < cases.length; i++) {
      long ts = cases[i];
      TimeUtil.Date res = results[i];
      assertEquals(res, TimeUtil.convertToDate(ts));
    }
  }

  @Test
  public void testDecodeYear() {
    long Y2001_01_01 = 978307200000L;
    long Y1970_01_18 = 1513601777L;
    assertEquals(2001, TimeUtil.getYear(Y2001_01_01));
    assertEquals(2000, TimeUtil.getYear(Y2001_01_01 - 1));
    assertEquals(1970, TimeUtil.getYear(Y1970_01_18));
  }
}
