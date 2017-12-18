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

package com.pingcap.tikv.util;

/**
 * The Time util.
 * <p>
 * Used for converting a timestamp to timezone-insensitive {com.pingcap.tikv.util.Date}
 */
public class TimeUtil {
  /**
   * The type Date.
   */
  public static class Date {
    private int year = -1;
    private int month = -1;
    private int day = -1;

    /**
     * Gets year.
     *
     * @return the year
     */
    public int getYear() {
      return year;
    }

    /**
     * Gets month.
     *
     * @return the month
     */
    public int getMonth() {
      return month;
    }

    /**
     * Gets day.
     *
     * @return the day
     */
    public int getDay() {
      return day;
    }

    /**
     * Instantiates a new Date.
     *
     * @param year  the year
     * @param month the month
     * @param day   the day
     */
    public Date(int year, int month, int day) {
      this.year = year;
      this.month = month;
      this.day = day;
    }

    private Date() {
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj instanceof Date) {
        Date date = (Date) obj;
        return date.year == year &&
            date.month == month &&
            date.day == day;
      }
      return false;
    }

    @Override
    public int hashCode() {
      int v = 7;
      v += v * 31 + year;
      v += v * 31 + month;
      v += v * 31 + day;
      return v;
    }

    @Override
    public String toString() {
      return year + "-" + month + "-" + day;
    }
  }

  private static int START_YEAR = 1970;
  private static long MS_PER_DAY = 24 * 3600 * 1000;
  private static final int[] MON_AVERAGE_YEAR = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  private static final int[] MON_LEAP_YEAR = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  private static boolean isLeapYear(int year) {
    return year % 4 == 0 && (year % 400 == 0 || year % 100 != 0);
  }

  /**
   * Get year from timestamp using binary search.
   *
   * @param timestamp the timestamp
   * @return the result year
   */
  public static int getYear(long timestamp) {
    int l = 0, r = 9999;
    long days = timestamp / MS_PER_DAY;
    while (l <= r) {
      int mid = (l + r) >> 1;
      long from1970Days = calcDays(START_YEAR, mid);
      if (from1970Days > days) {
        r = mid - 1;
      } else if (from1970Days < days) {
        l = mid + 1;
      } else {
        return mid + 1;
      }
    }
    return l;
  }

  /**
   * Calculate how many days year y from A.D. 0
   *
   * @param y year
   * @return days passed
   */
  private static long calcDays(int y) {
    return 365 * y + (y / 4 - y / 400 + y / 100);
  }

  /**
   * Calculate the days between tow year: from and to
   *
   * @param from beginning year
   * @param to   ending year
   * @return days between the two years
   */
  private static long calcDays(int from, int to) {
    return calcDays(to) - calcDays(from - 1);
  }

  private static void setMonthDay(int curYear, long timestamp, Date result) {
    int[] MONS;
    long daysInYearMs = timestamp - calcDays(START_YEAR, curYear - 1) * MS_PER_DAY;
    if (isLeapYear(curYear)) {
      MONS = MON_LEAP_YEAR;
    } else {
      MONS = MON_AVERAGE_YEAR;
    }

    for (int i = 0; i < MONS.length; i++) {
      long monMs = MONS[i] * MS_PER_DAY;
      if (daysInYearMs < monMs) {
        result.month = i + 1;
        for (int j = 1; j <= MONS[i]; j++) {
          if (daysInYearMs < MS_PER_DAY) {
            result.day = j;
            return;
          } else {
            daysInYearMs -= MS_PER_DAY;
          }
        }
      } else {
        daysInYearMs -= monMs;
      }
    }

    throw new RuntimeException("Unexpected days in year " + curYear);
  }

  private static Date validate(Date date) {
    if (date.year < 0 ||
        date.day < 0 ||
        date.month < 0) {
      throw new RuntimeException("Invalid date");
    }
    return date;
  }

  /**
   * Convert timestamp to Date.
   *
   * @param timestamp the timestamp
   * @return the result date
   */
  public static Date convertToDate(long timestamp) {
    Date result = new Date();
    int year = getYear(timestamp);
    result.year = year;

    setMonthDay(year, timestamp, result);
    return validate(result);
  }
}
