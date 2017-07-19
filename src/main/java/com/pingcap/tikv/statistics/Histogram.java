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
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
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

public class Histogram {

    private final String DBNAME="mysql"; //the name of database
    private final String Table="stats_buckets"; //the name of table
    private final String Table_ID="table_id"; //the ID of table
    private final String IS_Index="is_index"; // whether or not have an index
    private final String Hist_ID="hist_id";   //Column ID for each histogram
    private final String Bucket_ID="bucket_id"; //the ID of bucket
    private final String COUNT="count";       //the total number of bucket
    private final String REPEATS="repeats"; //repeats values in histogram
    private final String Lower_Bound="lower_bound"; //lower bound of histogram
    private final String Upper_Bound="upper_bound"; //upper bound of histogram
    private final long distinctValueOfNumber = 0; // Number of distinct values.
    public Bucket[] buckets ;

    private static TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    private static TiCluster cluster = TiCluster.getCluster(conf);
    private static Snapshot snapshot = cluster.createSnapshot();

    // histogramFromStorage from the storage to histogram.
    public void histogramFromStorage(long tableID,long isIndex,long colID){
        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase(DBNAME);
        TiTableInfo table = cat.getTable(db, Table);
        TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);

        /*
            select bucket_id, count, repeats, lower_bound, upper_bound from mysql.x
            where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, colID
         */
        List<TiExpr> firstAnd = ImmutableList.of(
                new Equal(TiColumnRef.create(Table_ID, table), TiConstant.create(tableID)),
                new Equal(TiColumnRef.create(IS_Index, table), TiConstant.create(isIndex)),
                new Equal(TiColumnRef.create(Hist_ID, table), TiConstant.create(colID))
        );

        ScanBuilder scanBuilder = new ScanBuilder();
        ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, table);

        /*
        mysql> select * from mysql.stats_buckets;
            +----------+----------+---------+-----------+-------+---------+-------------+-------------+
            | table_id | is_index | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
            +----------+----------+---------+-----------+-------+---------+-------------+-------------+
            |       25 |        0 |       1 |         0 |     3 |       3 | 1           | 1           |
            |       25 |        0 |       1 |         1 |     1 |       1 | 2           | 2           |
            |       25 |        0 |       1 |         2 |     3 |       3 | 4           | 4           |
            |       25 |        0 |       1 |         3 |     1 |       1 | 5           | 5           |
            +----------+----------+---------+-----------+-------+---------+-------------+-------------+
            7 rows in set (0.01 sec)
         */
        TiSelectRequest selReq = new TiSelectRequest();
        selReq.addRanges(scanPlan.getKeyRanges())
                .setTableInfo(table)
                .setIndexInfo(index)
                .addField(TiColumnRef.create(Table_ID, table))
                .addField(TiColumnRef.create(IS_Index, table))
                .addField(TiColumnRef.create(Hist_ID, table))
                .addField(TiColumnRef.create(Bucket_ID, table))
                .addField(TiColumnRef.create(COUNT, table))
                .addField(TiColumnRef.create(REPEATS, table))
                .addField(TiColumnRef.create(Lower_Bound, table))
                .addField(TiColumnRef.create(Upper_Bound,table))
                .setStartTs(snapshot.getVersion());

        if (conf.isIgnoreTruncate()) {
            selReq.setTruncateMode(TiSelectRequest.TruncateMode.IgnoreTruncation);
        } else if (conf.isTruncateAsWarning()) {
            selReq.setTruncateMode(TiSelectRequest.TruncateMode.TruncationAsWarning);
        }

        selReq.addWhere(PredicateUtils.mergeCNFExpressions(scanPlan.getFilters()));


        List<RangeSplitter.RegionTask> keyWithRegionTasks =
                RangeSplitter.newSplitter(cluster.getRegionManager())
                        .splitRangeByRegion(selReq.getRanges());
        for (RangeSplitter.RegionTask worker : keyWithRegionTasks) {
            Iterator<Row> it = snapshot.select(selReq, worker);

            while (it.hasNext()) {
                Row row = it.next();
                SchemaInfer schemaInfer = SchemaInfer.create(selReq);
                long buckID = row.getLong(0);
                long count = row.getLong(1);
                long repeats = row.getLong(2);
                if(isIndex == 1) {
                    Comparable<ByteString> lowerBound = Comparables.wrap(row.getLong(3));
                }else {
                    Comparable<ByteString> lowerBound = (Comparable<ByteString>)
                            row.get(row.getInteger(3), DataTypeFactory.of(Types.TYPE_BLOB));
                    Comparable<ByteString> upperBound = (Comparable<ByteString>)
                            row.get(row.getInteger(4), DataTypeFactory.of(Types.TYPE_BLOB));
                }
                for (int i = 0; i < row.fieldCount(); i++) {
                    Object val = row.get(i, schemaInfer.getType(i));
                }
            }
        }
    }

    // equalRowCount estimates the row count where the column equals to value.
    private float equalRowCount(ByteString values){
        Bucket bucket=new Bucket();
        int index = bucket.lowerBound.compareTo(values);
        if (index == buckets.length){
            return 0;
        }
        Comparable comparable = Comparables.wrap(values);
        float c = comparable.compareTo(bucket.lowerBound);
        if (c < 0){
            return 0;
        }
        return totalRowCount()/distinctValueOfNumber;
    }

    // greaterRowCount estimates the row count where the column greater than value.
    private float greaterRowCount(ByteString values){
        float lessCount = lessRowCount(values);
        float equalCount = equalRowCount(values);
        float greaterCount;
        greaterCount = totalRowCount() - lessCount - equalCount;
        if (greaterCount < 0){
            greaterCount = 0;
        }
        return greaterCount;
    }

    // greaterAndEqRowCount estimates the row count where the column less than or equal to value.
    public float greaterAndEqRowCount(ByteString values){
        float greaterCount = greaterRowCount(values);
        float equalCount = equalRowCount(values);
        return greaterCount + equalCount;
    }

    // lessRowCount estimates the row count where the column less than value.
    private float lessRowCount(ByteString values){
          Bucket bucket=new Bucket();
          int index = bucket.lowerBound.compareTo(values);
          if (index == buckets.length){
              return 0;
          }
          float currentCount = buckets[index].count;
          float previousCount = 0;
          if (index > 0){
              previousCount = buckets[index-1].count;
          }
          float lessThanBucketValueCount = currentCount - buckets[index].repeats;
          Comparable comparable = Comparables.wrap(values);
          float c = comparable.compareTo(bucket.lowerBound);
            if (c <= 0){
                return previousCount;
            }
          return (previousCount + lessThanBucketValueCount)/2;
    }

    // lessAndEqRowCount estimates the row count where the column less than or equal to value.
    public float lessAndEqRowCount(ByteString values){
        float lessCount = lessRowCount(values);
        float equalCount = equalRowCount(values);
        return lessCount + equalCount;
    }

    // betweenRowCount estimates the row count where column greater or equal to a and less than b.
    public float betweenRowCount(ByteString a,ByteString b){
       float lessCountA = lessRowCount(a);
       float lessCountB = lessRowCount(b);
       if (lessCountB >= lessCountA){
           return inBucketBetweenCount();
       }
       return lessCountB - lessCountA;
    }

    private float totalRowCount(){
        if (0 == buckets.length){
            return 0;
        }
        return (buckets[buckets.length-1].count);
    }

    private float bucketRowCount(){
        return totalRowCount()/buckets.length;
    }

    private float inBucketBetweenCount(){
        // TODO: Make this estimation more accurate using uniform spread assumption.
        return bucketRowCount()/3+1;
    }
}
