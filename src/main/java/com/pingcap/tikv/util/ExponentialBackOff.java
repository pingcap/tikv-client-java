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

import com.google.common.base.Preconditions;

public class ExponentialBackOff implements BackOff {
  private long firstFib = 1;
  private long secondFib = 1;
  private int attempts;
  private int counter;

  public ExponentialBackOff(int attempts) {
    Preconditions.checkArgument(attempts >= 1, "Retry count cannot be less than 1.");
    counter = 0;
    this.attempts = attempts;
  }

  @Override
  public void reset() {
    this.counter = 0;
    this.firstFib = 1;
    this.secondFib = 1;
  }

  /**
   * produces 0 1 1 2 3 ... fibonacci series number.
   */
  @Override
  public long nextBackOffMillis() {
    if(attempts < counter) {
      return BackOff.STOP;
    }
    // update two number in fibonacci's series
    // 0 1
    // 1 2
    long millis = firstFib * 1000;
    firstFib = secondFib;
    secondFib = firstFib + secondFib;
    counter++;
    return millis;
  }
}
