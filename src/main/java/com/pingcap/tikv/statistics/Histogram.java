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
 *
 */

package com.pingcap.tikv.statistics;

import com.pingcap.tikv.util.Bucket;

public class Histogram {

  //Histogram
  public long numberOfDistinctValue; // Number of distinct values.
  public Bucket[] buckets;
  public long nullCount;
  public long lastUpdateVersion;
  public long id;

  Histogram() {}

  public Histogram(
      long numberOfDistinctValue,
      Bucket[] buckets,
      long nullCount,
      long lastUpdateVersion,
      long id) {
    this.numberOfDistinctValue = numberOfDistinctValue;
    this.buckets = buckets;
    this.nullCount = nullCount;
    this.lastUpdateVersion = lastUpdateVersion;
    this.id = id;
  }

  public long getNumberOfDistinctValue() {
    return numberOfDistinctValue;
  }

  public void setNumberOfDistinctValue(long numberOfDistinctValue) {
    this.numberOfDistinctValue = numberOfDistinctValue;
  }

  public Bucket[] getBuckets() {
    return buckets;
  }

  public void setBuckets(Bucket[] buckets) {
    this.buckets = buckets;
  }

  public long getNullCount() {
    return nullCount;
  }

  public void setNullCount(long nullCount) {
    this.nullCount = nullCount;
  }

  public long getLastUpdateVersion() {
    return lastUpdateVersion;
  }

  public void setLastUpdateVersion(long lastUpdateVersion) {
    this.lastUpdateVersion = lastUpdateVersion;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }
}