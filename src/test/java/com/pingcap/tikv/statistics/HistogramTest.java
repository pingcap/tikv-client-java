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
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tikv.PDClient;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiCluster;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.operation.ChunkIterator;
import com.pingcap.tikv.operation.SchemaInfer;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.predicates.ScanBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.util.Bucket;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.RangeSplitter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.pingcap.tikv.types.Types.TYPE_BLOB;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by birdstorm on 2017/8/13.
 *
 */
public class HistogramTest {
    private static TiConfiguration conf =
            TiConfiguration.createDefault(ImmutableList.of("127.0.0.1:" + 2379));
    private static TiCluster cluster = TiCluster.getCluster(conf);
    private static Snapshot snapshot = cluster.createSnapshot();
    private static List<Chunk> chunks = new ArrayList<>();
    private static Histogram Histogram = new Histogram();

    @Test
    public void setup() throws Exception {
    /*
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * | table_id | is_index | hist_id | bucket_id | count | repeats | upper_bound | lower_bound |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     * |       27 |        0 |       1 |         0 |     1 |       1 | 2           | 1           |
     * |       27 |        0 |       1 |         1 |     1 |       1 | 5           | 3           |
     * |       27 |        1 |       2 |         0 |     1 |       1 | 1           | 1           |
     * +----------+----------+---------+-----------+-------+---------+-------------+-------------+
     *
     *
     */

        String histogramStr = "\b6\b\000\b\002\b\000\b\002\b\002\002\0022\002\0021\b6\b\000\b\002\b\002\b\002\b\002\002\0025\002\0023\b6" +
                "\b\002\b\004\b\000\b\002\b\002\002\0021\002\0021";
        Chunk chunk =
                Chunk.newBuilder()
                        .setRowsData(ByteString.copyFromUtf8(histogramStr))
                        .addRowsMeta(0, RowMeta.newBuilder().setHandle(6).setLength(18))
                        .addRowsMeta(1, RowMeta.newBuilder().setHandle(7).setLength(18))
                        .addRowsMeta(2, RowMeta.newBuilder().setHandle(8).setLength(18))
                        .build();

        chunks.add(chunk);
        ChunkIterator chunkIterator = new ChunkIterator(chunks);
        com.pingcap.tikv.types.DataType blobs = DataTypeFactory.of(TYPE_BLOB);
        com.pingcap.tikv.types.DataType ints = DataTypeFactory.of(TYPE_LONG);
        Row row = ObjectRowImpl.create(24);
        CodecDataInput cdi = new CodecDataInput(chunkIterator.next());
        ints.decodeValueToRow(cdi, row, 0);
        ints.decodeValueToRow(cdi, row, 1);
        ints.decodeValueToRow(cdi, row, 2);
        ints.decodeValueToRow(cdi, row, 3);
        ints.decodeValueToRow(cdi, row, 4);
        ints.decodeValueToRow(cdi, row, 5);
        blobs.decodeValueToRow(cdi, row, 6);
        blobs.decodeValueToRow(cdi, row, 7);
        cdi = new CodecDataInput(chunkIterator.next());
        ints.decodeValueToRow(cdi, row, 8);
        ints.decodeValueToRow(cdi, row, 9);
        ints.decodeValueToRow(cdi, row, 10);
        ints.decodeValueToRow(cdi, row, 11);
        ints.decodeValueToRow(cdi, row, 12);
        ints.decodeValueToRow(cdi, row, 13);
        blobs.decodeValueToRow(cdi, row, 14);
        blobs.decodeValueToRow(cdi, row, 15);
        cdi = new CodecDataInput(chunkIterator.next());
        ints.decodeValueToRow(cdi, row, 16);
        ints.decodeValueToRow(cdi, row, 17);
        ints.decodeValueToRow(cdi, row, 18);
        ints.decodeValueToRow(cdi, row, 19);
        ints.decodeValueToRow(cdi, row, 20);
        ints.decodeValueToRow(cdi, row, 21);
        blobs.decodeValueToRow(cdi, row, 22);
        blobs.decodeValueToRow(cdi, row, 23);


        assertEquals(row.getLong(0), 27);
        assertEquals(row.getLong(1), 0);
        assertEquals(row.getLong(2), 1);
        assertEquals(row.getLong(3), 0);
        assertEquals(row.getLong(4), 1);
        assertEquals(row.getLong(5), 1);
        assertArrayEquals(row.getBytes(6), ByteString.copyFromUtf8("2").toByteArray());
        assertArrayEquals(row.getBytes(7), ByteString.copyFromUtf8("1").toByteArray());
        assertEquals(row.getLong(8), 27);
        assertEquals(row.getLong(9), 0);
        assertEquals(row.getLong(10), 1);
        assertEquals(row.getLong(11), 1);
        assertEquals(row.getLong(12), 1);
        assertEquals(row.getLong(13), 1);
        assertArrayEquals(row.getBytes(14), ByteString.copyFromUtf8("5").toByteArray());
        assertArrayEquals(row.getBytes(15), ByteString.copyFromUtf8("3").toByteArray());
        assertEquals(row.getLong(16), 27);
        assertEquals(row.getLong(17), 1);
        assertEquals(row.getLong(18), 2);
        assertEquals(row.getLong(19), 0);
        assertEquals(row.getLong(20), 1);
        assertEquals(row.getLong(21), 1);
        assertArrayEquals(row.getBytes(22), ByteString.copyFromUtf8("1").toByteArray());
        assertArrayEquals(row.getBytes(23), ByteString.copyFromUtf8("1").toByteArray());
    }

    @Test
    public void testHistogram() throws Exception {
        Bucket[] buckets = new Bucket[4];
        Bucket bucket = new Bucket();
        bucket.setCount(2);
        bucket.setRepeats(1);
        bucket.setLowerBound(Comparables.wrap("1"));
        bucket.setUpperBound(Comparables.wrap("1"));

/*
        TiTableInfo tableInfo = cluster.getCatalog().getTable();
        RegionManager manager = new RegionManager(pdClient);
        Histogram histogram = Histogram.createHistogram(27, 0, 1, 2, 3,
                0, snapshot, DataTypeFactory.of(1), tableInfo, manager);

        //Histogram histogram = Histogram.createHistogram(27, 0, 1, snapshot, conf, cluster);
        histogram.setId(10);
        histogram.setBuckets(buckets);
        histogram.setLastUpdateVersion(1);
        histogram.setNullCount(0);
        histogram.setNumberOfDistinctValue(1);

        int index = 4;
        int length = histogram.getBuckets().length;
        assertEquals(index, length);
*/

        Logger log = Logger.getLogger("io.grpc");
        log.setLevel(Level.WARNING);
        PDClient client = PDClient.createRaw(cluster.getSession());
        for (int i = 0; i < 51; i++) {
            TiRegion r = client.getRegionByID(i);
            r.getId();
        }

        Catalog cat = cluster.getCatalog();
        TiDBInfo db = cat.getDatabase("mysql");
        TiTableInfo table = cat.getTable(db, "stats_buckets");
        TiIndexInfo index = TiIndexInfo.generateFakePrimaryKeyIndex(table);

        List<TiExpr> firstAnd =
                ImmutableList.of(
                        new Equal(TiColumnRef.create("table_id", table), TiConstant.create(27))
                        //new Equal(TiColumnRef.create("is_index", table), TiConstant.create(0)),
                        //new Equal(TiColumnRef.create("hist_id", table), TiConstant.create(1))
                );
        ScanBuilder scanBuilder = new ScanBuilder();
        ScanBuilder.ScanPlan scanPlan = scanBuilder.buildScan(firstAnd, index, table);
        TiSelectRequest selReq = new TiSelectRequest();
        selReq
                .addRanges(scanPlan.getKeyRanges())
                .setTableInfo(table)
                .addField(TiColumnRef.create("table_id", table))
                .addField(TiColumnRef.create("is_index", table))
                .addField(TiColumnRef.create("hist_id", table))
                .addField(TiColumnRef.create("bucket_id", table))
                .addField(TiColumnRef.create("count", table))
                .addField(TiColumnRef.create("repeats", table))
                .addField(TiColumnRef.create("upper_bound", table))
                .addField(TiColumnRef.create("lower_bound", table))
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
        for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
            Iterator<Row> it = snapshot.select(selReq, task);
            while (it.hasNext()) {
                Row r = it.next();
                SchemaInfer schemaInfer = SchemaInfer.create(selReq);
                for (int i = 0; i < r.fieldCount(); i++) {
                    Object val = r.get(i, schemaInfer.getType(i));
                    System.out.print(val.toString());
                    System.out.print(" ");
                }
                System.out.print("\n");
            }
        }


        cluster.close();
        client.close();
    }
}
