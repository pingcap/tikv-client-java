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

import com.google.protobuf.ByteString;

public class Bucket {
  public long count;
  public long repeats;
  public Comparable<Object> lowerBound;
  public Comparable<Object> upperBound;

  public Bucket(Comparable<Object> lowerBound, Comparable<Object> upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public Bucket() {}

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getRepeats() {
    return repeats;
  }

  public void setRepeats(long repeats) {
    this.repeats = repeats;
  }

  public Comparable<Object> getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(Comparable<Object> lowerBound) {
    this.lowerBound = lowerBound;
  }

  public Comparable<Object> getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(Comparable<Object> upperBound) {
    this.upperBound = upperBound;
  }
}