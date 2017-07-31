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

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DataType;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.operation.ChunkIterator;
import com.pingcap.tikv.row.ObjectRowImpl;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataTypeFactory;
import org.junit.Test;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TiHistogramTest {
  private List<Chunk> chunks = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    /*
     *rows_data:"\b6   \b\000  \b\002   \b\000   \b\002  \b\002\  002\0021\    002\0021
     *            27    0      1         0       1       1        [B@2ad48653   [B@6bb4dd34
	 *           \b6   \b\000  \b\002   \b\002   \b\002  \b\002   \002\0022    \002\0022"
     *            27    0        1       1        1        1       [B@7d9f158f  [B@45efd90f
     */
    String histogramStr =
        "\b6\b\000\b\002\b\000\b\002\b\002\002\0021\002\0021\b6\b\000\b\002\b\002\b\002\b\002\002\0022\002\0022";
    Chunk chunk =
        Chunk.newBuilder()
            .setRowsData(ByteString.copyFromUtf8(histogramStr))
            .addRowsMeta(0, RowMeta.newBuilder().setHandle(1).setLength(18))
            .addRowsMeta(1, RowMeta.newBuilder().setHandle(2).setLength(18))
            .build();
    chunks.add(chunk);
    ChunkIterator chunkIterator = new ChunkIterator(chunks);
    com.pingcap.tikv.types.DataType blobs = DataTypeFactory.of(TYPE_BLOB);
    com.pingcap.tikv.types.DataType ints = DataTypeFactory.of(TYPE_LONG);
    Row row = ObjectRowImpl.create(16);
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

    assertEquals(row.getLong(0), 27);
    assertEquals(row.getLong(1), 0);
    assertEquals(row.getLong(2), 1);
    assertEquals(row.getLong(3), 0);
    assertEquals(row.getLong(4), 1);
    assertEquals(row.getLong(5), 1);
    assertArrayEquals(row.getBytes(6),ByteString.copyFromUtf8("1").toByteArray());
    assertArrayEquals(row.getBytes(6),ByteString.copyFromUtf8("1").toByteArray());
    assertEquals(row.getLong(8), 27);
    assertEquals(row.getLong(9), 0);
    assertEquals(row.getLong(10), 1);
    assertEquals(row.getLong(11), 1);
    assertEquals(row.getLong(12), 1);
    assertEquals(row.getLong(13), 1);
    assertArrayEquals(row.getBytes(14), ByteString.copyFromUtf8("2").toByteArray());
    assertArrayEquals(row.getBytes(15), ByteString.copyFromUtf8("2").toByteArray());
  }

  /** Method: equalRowCount(ByteString values) */
  @Test
  public void testEqualRowCount() throws Exception {
    //get each one element of histogram from chunks
    chunks.get(0).getRowsData().toStringUtf8();
  }

  /** Method: greaterRowCount(ByteString values) */
  @Test
  public void testGreaterRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: greaterAndEqRowCount(ByteString values) */
  @Test
  public void testGreaterAndEqRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: lessRowCount(ByteString values) */
  @Test
  public void testLessRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: lessAndEqRowCount(ByteString values) */
  @Test
  public void testLessAndEqRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: betweenRowCount(ByteString a, ByteString b) */
  @Test
  public void testBetweenRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: totalRowCount() */
  @Test
  public void testTotalRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: bucketRowCount() */
  @Test
  public void testBucketRowCount() throws Exception {
    //TODO: Test goes here...
  }

  /** Method: inBucketBetweenCount() */
  @Test
  public void testInBucketBetweenCount() throws Exception {
    //TODO: Test goes here...
  }
}
