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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiCluster;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import com.pingcap.tikv.util.Bucket;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.RangeSplitter;

import java.util.Iterator;
import java.util.List;

public class TiHistogram {

  private static final String DB_NAME = "mysql"; //the name of database
  private static final String TABLE_NAME = "stats_buckets"; //the name of table
  private static final String TABLE_ID = "table_id"; //the ID of table
  private static final String IS_INDEX = "is_index"; // whether or not have an index
  private static final String HIST_ID = "hist_id"; //Column ID for each histogram
  private static final String BUCKET_ID = "bucket_id"; //the ID of bucket
  private static final String COUNT = "count"; //the total number of bucket
  private static final String REPEATS = "repeats"; //repeats values in histogram
  private static final String LOWER_BOUND = "lower_bound"; //lower bound of histogram
  private static final String UPPER_BOUND = "upper_bound"; //upper bound of histogram

  //Histogram
  public Histogram histogram = new Histogram();

  //Bucket
  public static Bucket bucket = new Bucket();

  //createHistogram is func of create Histogram information
  public static Histogram createHistogram (
    long tableID, long isIndex, long colID, Snapshot snapshot,TiConfiguration conf,TiCluster cluster) throws Exception{
    Catalog cat = cluster.getCatalog();
    TiDBInfo db = cat.getDatabase(DB_NAME);
    TiTableInfo table = cat.getTable(db, TABLE_NAME);
    TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);

    List<TiExpr> firstAnd =
        ImmutableList.of(
            new Equal(TiColumnRef.create(TABLE_ID, table), TiConstant.create(tableID)),
            new Equal(TiColumnRef.create(IS_INDEX, table), TiConstant.create(isIndex)),
            new Equal(TiColumnRef.create(HIST_ID, table), TiConstant.create(colID)));
    ScanBuilder scanBuilder = new ScanBuilder();
    ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, table);
    TiSelectRequest selReq = new TiSelectRequest();
    selReq
        .addRanges(scanPlan.getKeyRanges())
        .setTableInfo(table)
        .addField(TiColumnRef.create(TABLE_ID, table))
        .addField(TiColumnRef.create(IS_INDEX, table))
        .addField(TiColumnRef.create(HIST_ID, table))
        .addField(TiColumnRef.create(BUCKET_ID, table))
        .addField(TiColumnRef.create(COUNT, table))
        .addField(TiColumnRef.create(REPEATS, table))
        .addField(TiColumnRef.create(LOWER_BOUND, table))
        .addField(TiColumnRef.create(UPPER_BOUND, table))
        .setStartTs(snapshot.getVersion());

    selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));

    List<RangeSplitter.RegionTask> keyWithRegionTasks =
        RangeSplitter.newSplitter(cluster.getRegionManager())
            .splitRangeByRegion(selReq.getRanges());
    for (RangeSplitter.RegionTask worker : keyWithRegionTasks) {
      Iterator<Row> it = snapshot.select(selReq, worker);

      while (it.hasNext()) {
        SchemaInfer schemaInfer = SchemaInfer.create(selReq);
        Row row = it.next();
        long buckID = row.getLong(0);
        long count = row.getLong(1);
        long repeats = row.getLong(2);
        if (isIndex == 1) {
          bucket.lowerBound = Comparables.wrap(row.getLong(3));
          bucket.upperBound = Comparables.wrap(row.getLong(4));
        } else {
          bucket.lowerBound =
              (Comparable<Object>) row.get(3, DataTypeFactory.of(Types.TYPE_BLOB));
          bucket.upperBound =
              (Comparable<Object>) row.get(4, DataTypeFactory.of(Types.TYPE_BLOB));
        }
      }
    }
    return new Histogram();
  }

  // equalRowCount estimates the row count where the column equals to value.
  public float equalRowCount(Object values) {
    int index = bucket.lowerBound.compareTo(values);
    if (index == histogram.buckets.length) {
      return 0;
    }
    Comparable comparable = Comparables.wrap(values);
    float c = comparable.compareTo(bucket.lowerBound);
    if (c < 0) {
      return 0;
    }
    return totalRowCount() / histogram.numberOfDistinctValue;
  }

  // greaterRowCount estimates the row count where the column greater than value.
  public float greaterRowCount(Object values) {
    float lessCount = lessRowCount(values);
    float equalCount = equalRowCount(values);
    float greaterCount;
    greaterCount = totalRowCount() - lessCount - equalCount;
    if (greaterCount < 0) {
      greaterCount = 0;
    }
    return greaterCount;
  }

  // greaterAndEqRowCount estimates the row count where the column less than or equal to value.
  public float greaterAndEqRowCount(Object values) {
    float greaterCount = greaterRowCount(values);
    float equalCount = equalRowCount(values);
    return greaterCount + equalCount;
  }

  // lessRowCount estimates the row count where the column less than value.
  public float lessRowCount(Object values) {
    int index = bucket.lowerBound.compareTo(values);
    if (index == histogram.buckets.length) {
      return 0;
    }
    float currentCount = histogram.buckets[index].count;
    float previousCount = 0;
    if (index > 0) {
      previousCount = histogram.buckets[index - 1].count;
    }
    float lessThanBucketValueCount = currentCount - histogram.buckets[index].repeats;
    Comparable comparable = Comparables.wrap(values);
    float c = comparable.compareTo(bucket.lowerBound);
    if (c <= 0) {
      return previousCount;
    }
    return (previousCount + lessThanBucketValueCount) / 2;
  }

  // lessAndEqRowCount estimates the row count where the column less than or equal to value.
  public float lessAndEqRowCount(Object values) {
    float lessCount = lessRowCount(values);
    float equalCount = equalRowCount(values);
    return lessCount + equalCount;
  }

  // betweenRowCount estimates the row count where column greater or equal to a and less than b.
  public float betweenRowCount(Object a, Object b) {
    float lessCountA = lessRowCount(a);
    float lessCountB = lessRowCount(b);
    if (lessCountB >= lessCountA) {
      return inBucketBetweenCount();
    }
    return lessCountB - lessCountA;
  }

  public float totalRowCount() {
    if (0 == histogram.buckets.length) {
      return 0;
    }
    return (histogram.buckets[histogram.buckets.length - 1].count);
  }

  public float bucketRowCount() {
    return totalRowCount() / histogram.buckets.length;
  }

  public float inBucketBetweenCount() {
    // TODO: Make this estimation more accurate using uniform spread assumption.
    return bucketRowCount() / 3 + 1;
  }
}