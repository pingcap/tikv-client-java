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

import com.pingcap.tidb.tipb.Chunk;
import org.junit.Test;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class TiHistogramTest {

//  private static TiTableInfo createTable() {
//    return new MetaUtils.TableBuilder()
//        .name("stats_buckets")
//        .addColumn("table_id", DataTypeFactory.of(Types.TYPE_LONG), true)
//        .addColumn("is_index", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("hist_id", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("bucket_id", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("count", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("repeats", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("upper_bound", DataTypeFactory.of(Types.TYPE_LONG))
//        .addColumn("lower_bound", DataTypeFactory.of(Types.TYPE_LONG))
//        .build();
//  }
//  private final String stats_bucketsJson =
//      "\n"
//          + "{\n"
//          + "   \"table_id\": 25,\n"
//          + "   \"is_index\": 0,\n"
//          + "   \"hist_id\": 1,\n"
//          + "   \"bucket_id\": 0,\n"
//          + "   \"count\": 1,\n"
//          + "   \"repeats\": 1,\n"
//          + "   \"upper_bound\": 1,\n"
//          + "   \"lower_bound\": 1,\n"
//          + "}";
    private List<Histogram> histograms = new ArrayList<>();
@Before
public void setup() throws Exception {
    /*
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * | table_id | is_index | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * |       25 |        0 |       1 |         0 |     1 |       1 | 1           | 1           |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     */

}

/** 
* 
* Method: equalRowCount(ByteString values) 
* 
*/ 
@Test
public void testEqualRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: greaterRowCount(ByteString values) 
* 
*/ 
@Test
public void testGreaterRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: greaterAndEqRowCount(ByteString values) 
* 
*/ 
@Test
public void testGreaterAndEqRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: lessRowCount(ByteString values) 
* 
*/ 
@Test
public void testLessRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: lessAndEqRowCount(ByteString values) 
* 
*/ 
@Test
public void testLessAndEqRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: betweenRowCount(ByteString a, ByteString b) 
* 
*/ 
@Test
public void testBetweenRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: totalRowCount() 
* 
*/ 
@Test
public void testTotalRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: bucketRowCount() 
* 
*/ 
@Test
public void testBucketRowCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: inBucketBetweenCount() 
* 
*/ 
@Test
public void testInBucketBetweenCount() throws Exception { 
//TODO: Test goes here... 
} 


} 
